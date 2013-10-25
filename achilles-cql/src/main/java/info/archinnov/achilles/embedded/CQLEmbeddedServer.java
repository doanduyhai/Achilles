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
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.context.CQLDaoContext.ACHILLES_DML_STATEMENT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_HOST;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;

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
	private static final Object SEMAPHORE = new Object();
	private static final Logger LOGGER = LoggerFactory.getLogger(CQLEmbeddedServer.class);
	private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

	private static String entityPackages;
	private static boolean initialized = false;

	private static Session session;
	private static CQLPersistenceManagerFactory pmf;
	private static CQLPersistenceManager manager;


	public CQLEmbeddedServer(Map<String,Object> originalParameters) {
		synchronized (SEMAPHORE) {
			if (!initialized) {
                Map<String, Object> parameters = CassandraEmbeddedConfigParameters
                        .mergeWithDefaultParameters(originalParameters);
				startServer(parameters);
                CQLEmbeddedServer.entityPackages = (String) parameters.get(ENTITY_PACKAGES);
				initialize(parameters);
			}
		}
	}

	private void initialize(Map<String,Object> parameters) {

        Map<String,Object> achillesConfigMap = new HashMap<String, Object>();
        String keyspaceName = (String) parameters.get(KEYSPACE_NAME);

        String hostname;
        int cqlPort;

		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
			String[] split = cassandraHost.split(":");
            hostname = split[0];
            cqlPort = Integer.parseInt(split[1]);
		} else {
			hostname = DEFAULT_CASSANDRA_HOST;
            cqlPort = (Integer)parameters.get(CASSANDRA_CQL_PORT);
        }

        Cluster cluster = createCluster(hostname, cqlPort);
        createKeyspaceIfNeeded(cluster,keyspaceName);

        achillesConfigMap.put(CLUSTER_PARAM, cluster);
        achillesConfigMap.put(NATIVE_SESSION_PARAM, cluster.connect(keyspaceName));
        achillesConfigMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
        achillesConfigMap.put(KEYSPACE_NAME_PARAM, keyspaceName);
        achillesConfigMap.put(FORCE_CF_CREATION_PARAM, true);

		pmf = new CQLPersistenceManagerFactory(achillesConfigMap);
		manager = pmf.createPersistenceManager();
		session = manager.getNativeSession();
		initialized = true;
	}

	public CQLPersistenceManagerFactory getPersistenceManagerFactory() {
		return pmf;
	}

	public CQLPersistenceManager getPersistenceManager() {
		return manager;
	}

    private Cluster createCluster(String host, int cqlPort){
        return Cluster.builder()
               .addContactPoint(host)
               .withPort(cqlPort)
                .withCompression(SNAPPY)
                .withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy())
                .withRetryPolicy(Policies.defaultRetryPolicy())
                .withReconnectionPolicy(Policies.defaultReconnectionPolicy())
                .build();
    }

	private void createKeyspaceIfNeeded(Cluster cluster,String keyspaceName) {
        System.out.println("createKeyspaceIfNeeded "+Thread.currentThread().toString());
        final Session session = cluster.connect("system");
        final Row row = session
                .execute("SELECT count(1) FROM schema_keyspaces WHERE keyspace_name='"+ keyspaceName+"'")
                .one();
        if(row.getLong(0) != 1) {
            session.execute("CREATE keyspace "+keyspaceName+" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES=false");
        }
        session.shutdown();
    }

	public void truncateTable(String tableName) {
		String query = "TRUNCATE " + tableName;
		session.execute(new SimpleStatement(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
		DML_LOGGER.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", "  Simple query", query, "ALL");
	}
}
