/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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

import static com.datastax.driver.core.ProtocolOptions.Compression.SNAPPY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_CF_CREATION_PARAM;
import static info.archinnov.achilles.context.DaoContext.ACHILLES_DML_STATEMENT;
import static info.archinnov.achilles.embedded.CassandraConfig.cqlRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageSslRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.thriftRandomPort;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.BUILD_NATIVE_SESSION_ONLY;
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
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_HOST;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedServerStarter.CASSANDRA_EMBEDDED;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.Policies;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory;
import info.archinnov.achilles.validation.Validator;

public class CassandraEmbeddedServer {

    public static final String CASSANDRA_HOST = "cassandraHost";

    public static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedServer.class);

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private static final Object SEMAPHORE = new Object();

    private static int cqlPort;

    private static int thriftPort;

    private static boolean embeddedServerStarted = false;

    private static final Map<String, Boolean> KEYSPACE_BOOTSTRAP_MAP = new HashMap<String, Boolean>();

	private static final Map<String, Session> SESSIONS_MAP = new HashMap<String, Session>();

	private static final Map<String, PersistenceManagerFactory> FACTORIES_MAP = new HashMap<String, PersistenceManagerFactory>();

	private static final Map<String, PersistenceManager> MANAGERS_MAP = new HashMap<String, PersistenceManager>();

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEmbeddedServer.class);

	private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

	private static String entityPackages;

	public CassandraEmbeddedServer(Map<String, Object> originalParameters) {
		Map<String, Object> parameters = CassandraEmbeddedConfigParameters
				.mergeWithDefaultParameters(originalParameters);

		String keyspaceName = (String) parameters.get(KEYSPACE_NAME);
		String cassandraHost = System.getProperty(CASSANDRA_HOST);

		// No external Cassandra server, start an embedded instance
		if (StringUtils.isBlank(cassandraHost)) {
			synchronized (SEMAPHORE) {
				if (!embeddedServerStarted) {
					startServer(parameters);
				} else {
					Integer cqlPort = (Integer) parameters.get(CASSANDRA_CQL_PORT);
					Integer thriftPort = (Integer) parameters.get(CASSANDRA_THRIFT_PORT);
					if (cqlPort != null && CassandraEmbeddedServer.cqlPort != cqlPort.intValue()) {
						throw new IllegalArgumentException(
								String.format(
										"An embedded Cassandra server is already listening to CQL port '%s', the specified CQL port '%s' does not match",
										CassandraEmbeddedServer.cqlPort, cqlPort));
					} else {
						parameters.put(CASSANDRA_CQL_PORT, CassandraEmbeddedServer.cqlPort);
					}

					if (thriftPort != null && CassandraEmbeddedServer.thriftPort != thriftPort.intValue()) {
						throw new IllegalArgumentException(
								String.format(
										"An embedded Cassandra server is already listening to Thrift port '%s', the specified Thrift port '%s' does not match",
										CassandraEmbeddedServer.thriftPort, thriftPort));
					} else {
						parameters.put(CASSANDRA_THRIFT_PORT, CassandraEmbeddedServer.thriftPort);
					}

				}
			}
		}

		synchronized (KEYSPACE_BOOTSTRAP_MAP) {
			if (!KEYSPACE_BOOTSTRAP_MAP.containsKey(keyspaceName)) {
				CassandraEmbeddedServer.entityPackages = (String) parameters.get(ENTITY_PACKAGES);
				initialize(parameters);
				KEYSPACE_BOOTSTRAP_MAP.put(keyspaceName, true);
			}
		}
	}



	public PersistenceManagerFactory getPersistenceManagerFactory(String keyspaceName) {
		if (!FACTORIES_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find PersistenceManagerFactory for keyspace '%s'",
					keyspaceName));
		}
		return FACTORIES_MAP.get(keyspaceName);
	}

	public PersistenceManager getPersistenceManager(String keyspaceName) {
		if (!MANAGERS_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find PersistenceManager for keyspace '%s'",
					keyspaceName));
		}
		return MANAGERS_MAP.get(keyspaceName);
	}

	public Session getNativeSession(String keyspaceName) {
		if (!SESSIONS_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find Session for keyspace '%s'", keyspaceName));
		}
		return SESSIONS_MAP.get(keyspaceName);
	}

    public static int getThriftPort() {
        return thriftPort;
    }

    public static int getCqlPort() {
        return cqlPort;
    }

    public void truncateTable(String keyspaceName, String tableName) {
        String query = "TRUNCATE " + tableName;
        Session session = SESSIONS_MAP.get(keyspaceName);
        session.execute(new SimpleStatement(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
        DML_LOGGER.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", "  Simple query", query, "ALL");
    }


    private Cluster createCluster(String host, int cqlPort) {
		return Cluster.builder().addContactPoint(host).withPort(cqlPort).withCompression(SNAPPY)
				.withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy())
				.withRetryPolicy(Policies.defaultRetryPolicy())
				.withReconnectionPolicy(Policies.defaultReconnectionPolicy()).build();
	}

    private void initialize(Map<String, Object> parameters) {

        Map<String, Object> achillesConfigMap = new HashMap<String, Object>();

        String keyspaceName = extractAndValidateKeyspaceName(parameters);
        Boolean keyspaceDurableWrite = (Boolean) parameters.get(KEYSPACE_DURABLE_WRITE);
        Boolean nativeSessionOnly = (Boolean) parameters.get(BUILD_NATIVE_SESSION_ONLY);

        String hostname;
        int cqlPort;

        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            String[] split = cassandraHost.split(":");
            hostname = split[0];
            cqlPort = Integer.parseInt(split[1]);
        } else {
            hostname = DEFAULT_CASSANDRA_HOST;
            cqlPort = (Integer) parameters.get(CASSANDRA_CQL_PORT);
        }

        Cluster cluster = createCluster(hostname, cqlPort);
        createKeyspaceIfNeeded(cluster, keyspaceName, keyspaceDurableWrite);

        if (nativeSessionOnly) {
            SESSIONS_MAP.put(keyspaceName, cluster.connect(keyspaceName));
        } else {
            achillesConfigMap.put(CLUSTER_PARAM, cluster);
            achillesConfigMap.put(NATIVE_SESSION_PARAM, cluster.connect(keyspaceName));
            achillesConfigMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
            achillesConfigMap.put(KEYSPACE_NAME_PARAM, keyspaceName);
            achillesConfigMap.put(FORCE_CF_CREATION_PARAM, true);

            PersistenceManagerFactory factory = new PersistenceManagerFactory(achillesConfigMap);
            PersistenceManager manager = factory.createPersistenceManager();

            FACTORIES_MAP.put(keyspaceName, factory);
            MANAGERS_MAP.put(keyspaceName, manager);
            SESSIONS_MAP.put(keyspaceName, manager.getNativeSession());
        }
    }

	private void createKeyspaceIfNeeded(Cluster cluster, String keyspaceName, Boolean keyspaceDurableWrite) {
		final Session session = cluster.connect("system");
		final Row row = session.execute(
				"SELECT count(1) FROM schema_keyspaces WHERE keyspace_name='" + keyspaceName + "'").one();
		if (row.getLong(0) != 1) {
			StringBuilder createKeyspaceStatement = new StringBuilder("CREATE keyspace ");
			createKeyspaceStatement.append(keyspaceName);
			createKeyspaceStatement.append(" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
			if (!keyspaceDurableWrite) {
				createKeyspaceStatement.append(" AND DURABLE_WRITES=false");
			}
			session.execute(createKeyspaceStatement.toString());
		}
		session.shutdown();
	}

    private void startServer(Map<String, Object> parameters) {
        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isBlank(cassandraHost)) {

            validateDataFolders(parameters);
            cleanCassandraDataFiles(parameters);
            cleanCassandraConfigFile(parameters);
            randomizePortsIfNeeded(parameters);

            CassandraConfig cassandraConfig = new CassandraConfig(parameters);

            // Start embedded server
            CASSANDRA_EMBEDDED.start(cassandraConfig);
            embeddedServerStarted = true;
        }
    }

    private String extractAndValidateKeyspaceName(Map<String, Object> parameters) {
        String keyspaceName = (String) parameters.get(KEYSPACE_NAME);
        Validator.validateNotBlank(keyspaceName, "The provided keyspace name should not be blank");
        Validator.validateTrue(KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches(),
                               "The provided keyspace name '%s' should match the " + "following pattern : '%s'", keyspaceName,
                               KEYSPACE_NAME_PATTERN.pattern());

        return keyspaceName;
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
            Validator.validateTrue(folder.canRead(), "No read credential. Please grant read permission for the current"
                    + " " + "user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
            Validator.validateTrue(folder.canWrite(), "No write credential. Please grant write permission for the "
                    + "current " + "user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
        }
    }

    private void cleanCassandraDataFiles(Map<String, Object> parameters) {
        if ((Boolean) parameters.get(CLEAN_CASSANDRA_DATA_FILES)) {
            final ImmutableSet<String> dataFolders = ImmutableSet.<String> builder()
                                                                 .add((String) parameters.get(DATA_FILE_FOLDER)).add((String) parameters.get(COMMIT_LOG_FOLDER))
                                                                 .add((String) parameters.get(SAVED_CACHES_FOLDER)).build();
            for (String dataFolder : dataFolders) {
                File dataFolderFile = new File(dataFolder);
                if (dataFolderFile.exists() && dataFolderFile.isDirectory()) {
                    log.info("Cleaning up embedded Cassandra data directory '{}'", dataFolderFile.getAbsolutePath());
                    FileUtils.deleteQuietly(dataFolderFile);
                }
            }
        }
    }

    private void cleanCassandraConfigFile(Map<String, Object> parameters) {
        if ((Boolean) parameters.get(CLEAN_CASSANDRA_CONFIG_FILE)) {
            String configYamlFilePath = (String) parameters.get(CONFIG_YAML_FILE);
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

    private void randomizePortsIfNeeded(Map<String, Object> parameters) {
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

        CassandraEmbeddedServer.cqlPort = cqlPort;
        CassandraEmbeddedServer.thriftPort = thriftPort;
    }

    private Integer extractAndValidatePort(Object port, String portLabel) {
        Validator.validateTrue(port instanceof Integer, "The provided '%s' port should be an integer", portLabel);
        Validator.validateTrue((Integer) port > 0, "The provided '%s' port should positive", portLabel);
        return (Integer) port;

    }
}
