/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 *
 * Modified version of original class from HouseScream
 *
 * https://github.com/housecream/server/blob/develop/server/ws/src/main/java/org/housecream/server/application/CassandraEmbedded.java
 *
 */

package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.CassandraConfig.cqlRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageSslRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.thriftRandomPort;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_SSL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_THRIFT_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_CONFIG_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.COMMIT_LOG_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CONFIG_YAML_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DATA_FILE_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_ACHILLES_TEST_FOLDERS;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_ACHILLES_TEST_TRIGGERS_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.TypedMap;

public enum ServerStarter {
    CASSANDRA_EMBEDDED;

    private final Logger log = LoggerFactory.getLogger(ServerStarter.class);

    private static int cqlPort;

    private static int thriftPort;

    private static final OrderedShutdownHook orderedShutdownHook = new OrderedShutdownHook();

    public void startServer(String cassandraHost, TypedMap parameters) {
        if (StringUtils.isBlank(cassandraHost)) {

            log.debug("Do start embedded Cassandra server ");
            validateDataFolders(parameters);
            cleanCassandraDataFiles(parameters);
            cleanCassandraConfigFile(parameters);
            randomizePortsIfNeeded(parameters);

            CassandraConfig cassandraConfig = new CassandraConfig(parameters);

            // Start embedded server
            CASSANDRA_EMBEDDED.start(cassandraConfig);
        }
    }

    public void checkAndConfigurePorts(TypedMap parameters) {
        log.trace("Check and configure Thrift/CQL3 ports");
        Integer cqlPort = parameters.getTyped(CASSANDRA_CQL_PORT);
        Integer thriftPort = parameters.getTyped(CASSANDRA_THRIFT_PORT);
        if (cqlPort != null && ServerStarter.cqlPort != cqlPort.intValue()) {
            throw new IllegalArgumentException(String.format("An embedded Cassandra server is already listening to CQL port '%s', the specified CQL port '%s' does not match", ServerStarter.cqlPort, cqlPort));
        } else {
            parameters.put(CASSANDRA_CQL_PORT, ServerStarter.cqlPort);
        }

        if (thriftPort != null && ServerStarter.thriftPort != thriftPort.intValue()) {
            throw new IllegalArgumentException(String.format("An embedded Cassandra server is already listening to Thrift port '%s', the specified Thrift port '%s' does not match", ServerStarter.thriftPort, thriftPort));
        } else {
            parameters.put(CASSANDRA_THRIFT_PORT, ServerStarter.thriftPort);
        }
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public int getCQLPort() {
        return cqlPort;
    }

    public OrderedShutdownHook getShutdownHook() {
        return orderedShutdownHook;
    }

    private void start(final CassandraConfig config) {
        if (isAlreadyRunning()) {
            log.debug("Cassandra is already running, not starting new one");
            return;
        }

        final String triggersDir = createTriggersFolder();

        log.info(" Random embedded Cassandra RPC port/Thrift port = {}", config.getRPCPort());
        log.info(" Random embedded Cassandra Native port/CQL3 port = {}", config.getCqlPort());
        log.info(" Random embedded Cassandra Storage port = {}", config.getStoragePort());
        log.info(" Random embedded Cassandra Storage SSL port = {}", config.getStorageSSLPort());
        log.info(" Embedded Cassandra triggers directory = {}", triggersDir);

        log.info("Starting Cassandra...");
        config.write();

        System.setProperty("cassandra.triggers_dir", triggersDir);
        System.setProperty("cassandra.config", "file:" + config.getConfigFile().getAbsolutePath());
        System.setProperty("cassandra-foreground", "true");

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                CassandraDaemon cassandraDaemon = new CassandraDaemon();
                cassandraDaemon.activate();
                startupLatch.countDown();
            }
        });


        try {
            startupLatch.await(30, SECONDS);
        } catch (InterruptedException e) {
            log.error("Timeout starting Cassandra embedded", e);
            throw new IllegalStateException("Timeout starting Cassandra embedded", e);
        }

        // Generate an OrderedShutdownHook to shutdown all connections from java clients before closing the server
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Calling shutdown on all Cluster instances");
                // First call shutdown on all registered Java driver Cluster instances
                orderedShutdownHook.callShutDown();

                log.info("Shutting down embedded Cassandra server");
                // Then shutdown the server
                executor.shutdown();
            }
        });
    }

    private void validateDataFolders(Map<String, Object> parameters) {
        final String dataFolder = (String) parameters.get(DATA_FILE_FOLDER);
        final String commitLogFolder = (String) parameters.get(COMMIT_LOG_FOLDER);
        final String savedCachesFolder = (String) parameters.get(SAVED_CACHES_FOLDER);

        log.debug(" Embedded Cassandra data directory = {}", dataFolder);
        log.debug(" Embedded Cassandra commitlog directory = {}", commitLogFolder);
        log.debug(" Embedded Cassandra saved caches directory = {}", savedCachesFolder);

        validateFolder(dataFolder);
        validateFolder(commitLogFolder);
        validateFolder(savedCachesFolder);

    }

    private void validateFolder(String folderPath) {
        String currentUser = System.getProperty("user.name");
        final File folder = new File(folderPath);
        if (!DEFAULT_ACHILLES_TEST_FOLDERS.contains(folderPath)) {
            Validator.validateTrue(folder.exists(), "Folder '%s' does not exist", folder.getAbsolutePath());
            Validator.validateTrue(folder.isDirectory(), "Folder '%s' is not a directory", folder.getAbsolutePath());
            Validator.validateTrue(folder.canRead(), "No read credential. Please grant read permission for the current user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
            Validator.validateTrue(folder.canWrite(), "No write credential. Please grant write permission for the current user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
        } else if (!folder.exists()) {
            try {
                FileUtils.forceMkdir(folder);
            } catch (IOException e) {
                throw new RuntimeException("Cannot create Cassandra data folder " + folderPath, e);
            }
        }
    }

    private void cleanCassandraDataFiles(TypedMap parameters) {
        if (parameters.getTyped(CLEAN_CASSANDRA_DATA_FILES)) {
            final ImmutableSet<String> dataFolders = ImmutableSet.<String>builder()
                    .add(parameters.<String>getTyped(DATA_FILE_FOLDER))
                    .add(parameters.<String>getTyped(COMMIT_LOG_FOLDER))
                    .add(parameters.<String>getTyped(SAVED_CACHES_FOLDER)).build();
            for (String dataFolder : dataFolders) {
                File dataFolderFile = new File(dataFolder);
                if (dataFolderFile.exists() && dataFolderFile.isDirectory()) {
                    log.info("Cleaning up embedded Cassandra data directory '{}'", dataFolderFile.getAbsolutePath());
                    try {
                        FileUtils.cleanDirectory(dataFolderFile);
                    } catch (IOException e) {
                        throw new AchillesException(String.format("Cannot clean data folder %s", dataFolder));
                    }
                }
            }
        }
    }

    private void cleanCassandraConfigFile(TypedMap parameters) {
        if (parameters.getTyped(CLEAN_CASSANDRA_CONFIG_FILE)) {
            String configYamlFilePath = parameters.getTyped(CONFIG_YAML_FILE);
            final File configYamlFile = new File(configYamlFilePath);
            if (configYamlFile.exists()) {
                String currentUser = System.getProperty("user.name");
                Validator.validateTrue(configYamlFile.canWrite(),
                        "No write credential. Please grant write permission for "
                                + "the current user '%s' on file '%s'", currentUser, configYamlFile.getAbsolutePath());
                configYamlFile.delete();
            }
        }
    }

    private void randomizePortsIfNeeded(TypedMap parameters) {
        final Integer thriftPort = extractAndValidatePort(Optional.fromNullable(parameters.get(CASSANDRA_THRIFT_PORT))
                .or(thriftRandomPort()), CASSANDRA_THRIFT_PORT);
        final Integer cqlPort = extractAndValidatePort(
                Optional.fromNullable(parameters.get(CASSANDRA_CQL_PORT)).or(cqlRandomPort()), CASSANDRA_CQL_PORT);
        final Integer storagePort = extractAndValidatePort(Optional
                .fromNullable(parameters.get(CASSANDRA_STORAGE_PORT)).or(storageRandomPort()), CASSANDRA_STORAGE_PORT);
        final Integer storageSSLPort = extractAndValidatePort(
                Optional.fromNullable(parameters.get(CASSANDRA_STORAGE_SSL_PORT)).or(storageSslRandomPort()),
                CASSANDRA_STORAGE_SSL_PORT);

        parameters.put(CASSANDRA_THRIFT_PORT, thriftPort);
        parameters.put(CASSANDRA_CQL_PORT, cqlPort);
        parameters.put(CASSANDRA_STORAGE_PORT, storagePort);
        parameters.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        ServerStarter.cqlPort = cqlPort;
        ServerStarter.thriftPort = thriftPort;
    }

    private Integer extractAndValidatePort(Object port, String portLabel) {
        Validator.validateTrue(port instanceof Integer, "The provided '%s' port should be an integer", portLabel);
        Validator.validateTrue((Integer) port > 0, "The provided '%s' port should positive", portLabel);
        return (Integer) port;

    }

    private String createTriggersFolder() {
        log.trace("Create triggers folder");
        final File triggersDir = new File(System.getProperty("java.io.tmpdir") + DEFAULT_ACHILLES_TEST_TRIGGERS_FOLDER);
        if (!triggersDir.exists()) {
            triggersDir.mkdir();
        }
        return triggersDir.getAbsolutePath();
    }

    private boolean isAlreadyRunning() {
        log.trace("Check whether an embedded Cassandra is already running");
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

}
