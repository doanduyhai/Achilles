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

package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.COMPRESSION_TYPE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.LOAD_BALANCING_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RECONNECTION_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RETRY_POLICY;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.BUILD_NATIVE_SESSION_ONLY;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_HOST;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;
import static info.archinnov.achilles.embedded.StateRepository.REPOSITORY;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.persistence.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;
import info.archinnov.achilles.type.TypedMap;

public class AchillesInitializer {

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    void initializeFromParameters(String cassandraHost, TypedMap parameters) {
        String keyspaceName = parameters.getTyped(KEYSPACE_NAME_PARAM);
        synchronized (REPOSITORY) {
            if (!REPOSITORY.keyspaceAlreadyBootstrapped(keyspaceName)) {
                initialize(cassandraHost, parameters);
                REPOSITORY.markKeyspaceAsBootstrapped(keyspaceName);
            }
        }
    }

    private void initialize(String cassandraHost, TypedMap parameters) {

        String keyspaceName = extractAndValidateKeyspaceName(parameters);
        Boolean keyspaceDurableWrite = parameters.getTyped(KEYSPACE_DURABLE_WRITE);
        Boolean nativeSessionOnly = parameters.getTyped(BUILD_NATIVE_SESSION_ONLY);

        String hostname;
        int cqlPort;

        if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            String[] split = cassandraHost.split(":");
            hostname = split[0];
            cqlPort = Integer.parseInt(split[1]);
        } else {
            hostname = DEFAULT_CASSANDRA_HOST;
            cqlPort = parameters.getTyped(CASSANDRA_CQL_PORT);
        }

        Cluster cluster = createCluster(hostname, cqlPort, parameters);
        createKeyspaceIfNeeded(cluster, keyspaceName, keyspaceDurableWrite);

        if (nativeSessionOnly) {
            REPOSITORY.addNewSessionToKeyspace(keyspaceName, cluster.connect(keyspaceName));
        } else {
            Session nativeSession = cluster.connect(keyspaceName);
            parameters.put(ConfigurationParameters.CLUSTER_PARAM, cluster);
            parameters.put(ConfigurationParameters.NATIVE_SESSION_PARAM, nativeSession);
            parameters.put(ConfigurationParameters.KEYSPACE_NAME_PARAM, keyspaceName);
            if (!parameters.containsKey(ConfigurationParameters.FORCE_TABLE_CREATION_PARAM)) {
                parameters.put(ConfigurationParameters.FORCE_TABLE_CREATION_PARAM, true);
            }
            parameters.put(ConfigurationParameters.CLUSTER_PARAM, cluster);

            PersistenceManagerFactory factory = PersistenceManagerFactoryBuilder.build(parameters);

            PersistenceManager manager = factory.createPersistenceManager();

            REPOSITORY.addNewManagerFactoryToKeyspace(keyspaceName, factory);
            REPOSITORY.addNewManagerToKeyspace(keyspaceName, manager);
            REPOSITORY.addNewSessionToKeyspace(keyspaceName, manager.getNativeSession());
        }
    }

    private String extractAndValidateKeyspaceName(TypedMap parameters) {
        String keyspaceName = parameters.getTyped(KEYSPACE_NAME_PARAM);
        Validator.validateNotBlank(keyspaceName, "The provided keyspace name should not be blank");
        Validator.validateTrue(KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches(),
                "The provided keyspace name '%s' should match the " + "following pattern : '%s'", keyspaceName,
                KEYSPACE_NAME_PATTERN.pattern());

        return keyspaceName;
    }

    private Cluster createCluster(String host, int cqlPort, TypedMap parameters) {
        String clusterName = parameters.getTyped(CLUSTER_NAME_PARAM);
        Compression compression = parameters.getTyped(COMPRESSION_TYPE);
        LoadBalancingPolicy loadBalancingPolicy = parameters.getTyped(LOAD_BALANCING_POLICY);
        RetryPolicy retryPolicy = parameters.getTyped(RETRY_POLICY);
        ReconnectionPolicy reconnectionPolicy = parameters.getTyped(RECONNECTION_POLICY);

        return Cluster.builder().addContactPoint(host).withPort(cqlPort).withClusterName(clusterName)
                .withCompression(compression).withLoadBalancingPolicy(loadBalancingPolicy).withRetryPolicy(retryPolicy)
                .withReconnectionPolicy(reconnectionPolicy).build();
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
        session.close();
    }
}
