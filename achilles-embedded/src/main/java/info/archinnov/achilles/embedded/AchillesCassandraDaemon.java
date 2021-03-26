/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.embedded;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class AchillesCassandraDaemon extends CassandraDaemon {

    private static final Logger logger = LoggerFactory.getLogger(AchillesCassandraDaemon.class);
    private NativeTransportService nativeTransportService;
    /**
     * Override the default setup process to speed up bootstrap
     *
     * - disable JMX
     * - disable legacy schema migration
     * - no pre-3.0 hints migration
     * - no pre-3.0 batch entries migration
     * - disable auto compaction on all keyspaces (your test data should fit in memory!!!)
     * - disable metrics
     * - disable GCInspector
     * - disable mlock
     * - disable Thrift server
     * - disable startup checks (Jemalloc, validLaunchDate, JMXPorts, JvmOptions, JnaInitialization, initSigarLibrary, dataDirs, SSTablesFormat, SystemKeyspaceState, Datacenter, Rack)
     * - disable materialized view rebuild (you should clean data folder between each test anyway)
     * - disable the SizeEstimatesRecorder (estimate SSTable size, who cares for unit testing ?)
     */
    @Override
    protected void setup()  {
        // Delete any failed snapshot deletions on Windows - see CASSANDRA-9658
        if (FBUtilities.isWindows)
            WindowsFailedSnapshotTracker.deleteOldSnapshots();

        ThreadAwareSecurityManager.install();

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            StorageMetrics.exceptions.inc();
            logger.error("Exception in thread {}", t, e);
            Tracing.trace("Exception in thread {}", t, e);
            for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
            {
                JVMStabilityInspector.inspectThrowable(e2);

                if (e2 instanceof FSError)
                {
                    if (e2 != e) // make sure FSError gets logged exactly once.
                        logger.error("Exception in thread {}", t, e2);
                    FileUtils.handleFSError((FSError) e2);
                }

                if (e2 instanceof CorruptSSTableException)
                {
                    if (e2 != e)
                        logger.error("Exception in thread " + t, e2);
                    FileUtils.handleCorruptSSTable((CorruptSSTableException) e2);
                }
            }
        });

        // Populate token metadata before flushing, for token-aware sstable partitioning (#6696)
        StorageService.instance.populateTokenMetadata();

        // load schema from disk
        Schema.instance.loadFromDisk();

        // clean up debris in the rest of the keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspaceName.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                continue;

            for (CFMetaData cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                try
                {
                    ColumnFamilyStore.scrubDataDirectories(cfm);
                }
                catch (StartupException e)
                {
                    exitOrFail(e.returnCode, e.getMessage(), e.getCause());
                }
            }
        }

        Keyspace.setInitialized();

        // initialize keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace {}", keyspaceName);
            // disable auto compaction until commit log replay ends
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                for (ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.disableAutoCompaction();
                }
            }
        }


        try
        {
            loadRowAndKeyCacheAsync().get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Error loading key or row cache", t);
        }

        // replay the log if necessary
        try
        {
            CommitLog.instance.recoverSegmentsOnDisk();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // Re-populate token metadata after commit log recover (new peers might be loaded onto system keyspace #10293)
        StorageService.instance.populateTokenMetadata();

        // Disable auto compaction
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    if (store.getCompactionStrategyManager().shouldBeEnabled())
                        store.disableAutoCompaction();
                }
            }
        }

        SystemKeyspace.finishStartup();

        // start server internals
        StorageService.instance.registerDaemon(this);
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            exitOrFail(1, "Fatal configuration error", e);
        }

        // Native transport
        nativeTransportService = new NativeTransportService();

        completeSetup();
    }

    private void exitOrFail(int code, String message, Throwable cause)
    {
        logger.error(message, cause);
        System.exit(code);
    }

    private ListenableFuture<?> loadRowAndKeyCacheAsync()
    {
        final ListenableFuture<Integer> keyCacheLoad = CacheService.instance.keyCache.loadSavedAsync();

        final ListenableFuture<Integer> rowCacheLoad = CacheService.instance.rowCache.loadSavedAsync();

        @SuppressWarnings("unchecked")
        ListenableFuture<List<Integer>> retval = Futures.successfulAsList(keyCacheLoad, rowCacheLoad);

        return retval;
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     *
     * Hook for JSVC
     */
    @Override
    public void start()
    {
        startNativeTransport();
        StorageService.instance.setRpcReady(true);
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC / Procrun
     */
    @Override
    public void stop()
    {
        // On linux, this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        if (nativeTransportService != null)
            nativeTransportService.destroy();
        StorageService.instance.setRpcReady(false);

        // On windows, we need to stop the entire system as prunsrv doesn't have the jsvc hooks
        // We rely on the shutdown hook to drain the node
        if (FBUtilities.isWindows)
            System.exit(0);
    }

    @Override
    public void startNativeTransport()
    {
        if (nativeTransportService == null)
            throw new IllegalStateException("setup() must be called first for CassandraDaemon");
        else
            nativeTransportService.start();
    }

    @Override
    public void stopNativeTransport()
    {
        if (nativeTransportService != null)
            nativeTransportService.stop();
    }

    @Override
    public boolean isNativeTransportRunning()
    {
        return nativeTransportService != null ? nativeTransportService.isRunning() : false;
    }
}
