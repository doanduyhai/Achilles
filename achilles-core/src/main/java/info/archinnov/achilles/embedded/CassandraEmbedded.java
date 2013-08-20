/**
 *
 * Modified version of original class from HouseScream
 * 
 * https://github.com/housecream/server/blob/develop/server/ws/src/main/java/org/housecream/server/application/CassandraEmbedded.java
 * 
 */

package info.archinnov.achilles.embedded;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CassandraEmbedded {
    CASSANDRA_EMBEDDED;

    private final Logger log = LoggerFactory.getLogger(CassandraEmbedded.class);

    private ExecutorService executor;
    private CassandraConfig config;

    public void start(final CassandraConfig config) {
        this.config = config;

        final File cassandraHome = config.getCassandraHome();
        cleanUpExistingData(cassandraHome);

        if (isAlreadyRunning()) {
            log.info("Cassandra is already running, not starting new one");
            return;
        }

        log.info("Starting Cassandra...");
        config.write();
        System.setProperty("cassandra.config", "file:" + config.getConfigFile().getAbsolutePath());
        System.setProperty("cassandra-foreground", "true");

        final CountDownLatch startupLatch = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                CassandraDaemon cassandraDaemon = new CassandraDaemon();
                cassandraDaemon.activate();
                startupLatch.countDown();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                FileUtils.deleteQuietly(cassandraHome);
                log.info("Shutting down cassandra and cleaning embedded Cassandra home '{}'",
                        cassandraHome.getAbsolutePath());
            }
        });

        try {
            startupLatch.await(30, SECONDS);
        } catch (InterruptedException e) {
            log.error("Timeout starting Cassandra embedded", e);
            throw new IllegalStateException("Timeout starting Cassandra embedded", e);
        }
    }

    private boolean isAlreadyRunning() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            MBeanInfo mBeanInfo = mbs.getMBeanInfo(new ObjectName("org.apache.cassandra.db:type=StorageService"));
            if (mBeanInfo != null) {
                return true;
            }
            return false;
        } catch (InstanceNotFoundException e) {
            return false;
        } catch (IntrospectionException e) {
            throw new IllegalStateException("Cannot check if cassandra is already running", e);
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Cannot check if cassandra is already running", e);
        } catch (ReflectionException e) {
            throw new IllegalStateException("Cannot check if cassandra is already running", e);
        }

    }

    public CassandraConfig getConfig() {
        config.load();
        return config;
    }

    private void cleanUpExistingData(File cassandraHome)
    {
        if (cassandraHome.exists() && cassandraHome.isDirectory())
        {
            log.info("Cleaning up embedded Cassandra home '{}' before starting", cassandraHome.getAbsolutePath());
            FileUtils.deleteQuietly(cassandraHome);
        }
    }
}
