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
import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.context.CQLDaoContext.ACHILLES_DML_STATEMENT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.*;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.entity.manager.CQLPersistenceManagerFactory;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.Policies;

public class CQLEmbeddedServer extends AchillesEmbeddedServer {
	private static final Map<String, Boolean> KEYSPACE_BOOTSTRAP_MAP = new HashMap<String, Boolean>();

	private static final Map<String, Session> SESSIONS_MAP = new HashMap<String, Session>();

	private static final Map<String, CQLPersistenceManagerFactory> FACTORIES_MAP = new HashMap<String, CQLPersistenceManagerFactory>();

	private static final Map<String, CQLPersistenceManager> MANAGERS_MAP = new HashMap<String, CQLPersistenceManager>();

	private static final Logger LOGGER = LoggerFactory.getLogger(CQLEmbeddedServer.class);

	private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

	private static String entityPackages;

	public CQLEmbeddedServer(Map<String, Object> originalParameters) {
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
					if (cqlPort != null && AchillesEmbeddedServer.cqlPort != cqlPort.intValue()) {
						throw new IllegalArgumentException(
								String.format(
										"An embedded Cassandra server is already listening to CQL port '%s', the specified CQL port '%s' does not match",
										AchillesEmbeddedServer.cqlPort, cqlPort));
					} else {
						parameters.put(CASSANDRA_CQL_PORT, AchillesEmbeddedServer.cqlPort);
					}

					if (thriftPort != null && AchillesEmbeddedServer.thriftPort != thriftPort.intValue()) {
						throw new IllegalArgumentException(
								String.format(
										"An embedded Cassandra server is already listening to Thrift port '%s', the specified Thrift port '%s' does not match",
										AchillesEmbeddedServer.thriftPort, thriftPort));
					} else {
						parameters.put(CASSANDRA_THRIFT_PORT, AchillesEmbeddedServer.thriftPort);
					}

				}
			}
		}

		synchronized (KEYSPACE_BOOTSTRAP_MAP) {
			if (!KEYSPACE_BOOTSTRAP_MAP.containsKey(keyspaceName)) {
				CQLEmbeddedServer.entityPackages = (String) parameters.get(ENTITY_PACKAGES);
				initialize(parameters);
				KEYSPACE_BOOTSTRAP_MAP.put(keyspaceName, true);
			}
		}
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

			CQLPersistenceManagerFactory factory = new CQLPersistenceManagerFactory(achillesConfigMap);
			CQLPersistenceManager manager = factory.createPersistenceManager();

			FACTORIES_MAP.put(keyspaceName, factory);
			MANAGERS_MAP.put(keyspaceName, manager);
			SESSIONS_MAP.put(keyspaceName, manager.getNativeSession());
		}
	}

	public CQLPersistenceManagerFactory getPersistenceManagerFactory(String keyspaceName) {
		if (!FACTORIES_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find CQLPersistenceManagerFactory for keyspace '%s'",
					keyspaceName));
		}
		return FACTORIES_MAP.get(keyspaceName);
	}

	public CQLPersistenceManager getPersistenceManager(String keyspaceName) {
		if (!MANAGERS_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find CQLPersistenceManager for keyspace '%s'",
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

	private Cluster createCluster(String host, int cqlPort) {
		return Cluster.builder().addContactPoint(host).withPort(cqlPort).withCompression(SNAPPY)
				.withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy())
				.withRetryPolicy(Policies.defaultRetryPolicy())
				.withReconnectionPolicy(Policies.defaultReconnectionPolicy()).build();
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

	public void truncateTable(String keyspaceName, String tableName) {
		String query = "TRUNCATE " + tableName;
		Session session = SESSIONS_MAP.get(keyspaceName);
		session.execute(new SimpleStatement(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
		DML_LOGGER.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", "  Simple query", query, "ALL");
	}
}
