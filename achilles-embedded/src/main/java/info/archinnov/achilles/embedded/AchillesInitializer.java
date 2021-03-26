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

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.*;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.validation.Validator;

public class AchillesInitializer {

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static final Logger LOGGER = LoggerFactory.getLogger(AchillesInitializer.class);

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private Cluster singletonCluster;
    private Session singletonSession;

    void initializeFromParameters(String cassandraHost, TypedMap parameters) {
        synchronized (STARTED) {
            final String keyspaceName = extractAndValidateKeyspaceName(parameters);
            final Boolean durableWrite = parameters.getTyped(KEYSPACE_DURABLE_WRITE);
            if (STARTED.get() == false) {
                LOGGER.debug("Creating cluster and session singletons");
                singletonCluster = initializeCluster(cassandraHost, parameters);
                final Session tempSession = singletonCluster.connect();
                createKeyspaceIfNeeded(tempSession, keyspaceName, durableWrite);
                tempSession.close();
                singletonSession = singletonCluster.connect(keyspaceName);
                ServerStarter.CASSANDRA_EMBEDDED.getShutdownHook().addSession(singletonSession);
                executeStartupScripts(singletonSession, parameters);
                STARTED.getAndSet(true);
            } else {
                LOGGER.debug("Cluster and session singletons already created");
                createKeyspaceIfNeeded(singletonSession, keyspaceName, durableWrite);
                final boolean useSingletonSession = singletonSession.getLoggedKeyspace().toLowerCase()
                        .equals(keyspaceName.toLowerCase());

                Session tempSession = useSingletonSession
                        ? singletonSession
                        : singletonCluster.connect(keyspaceName);

                executeStartupScripts(tempSession, parameters);

                if (!useSingletonSession) {
                    tempSession.close();
                }
            }
        }
    }

    private Cluster initializeCluster(String cassandraHost, TypedMap parameters) {

        String hostname;
        int cqlPort;

        if (isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            String[] split = cassandraHost.split(":");
            hostname = split[0];
            cqlPort = Integer.parseInt(split[1]);
        } else {
            hostname = parameters.getTyped(RPC_ADDRESS);
            cqlPort = parameters.getTyped(CASSANDRA_CQL_PORT);
        }
        return createCluster(hostname, cqlPort, parameters);
    }


    private Cluster createCluster(String host, int cqlPort, TypedMap parameters) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating Cluster object with host/port {}/{} and parameters {}", host, cqlPort, parameters);
        }
        final String clusterName = parameters.getTyped(CLUSTER_NAME);
        final Compression compression = parameters.getTyped(COMPRESSION_TYPE);
        final LoadBalancingPolicy loadBalancingPolicy = parameters.getTyped(LOAD_BALANCING_POLICY);
        final RetryPolicy retryPolicy = parameters.getTyped(RETRY_POLICY);
        final ReconnectionPolicy reconnectionPolicy = parameters.getTyped(RECONNECTION_POLICY);
        final ProtocolVersion protocolVersion = parameters.getTypedOr(CASSANDRA_CONNECTION_PROTOCOL_VERSION,ProtocolVersion.NEWEST_SUPPORTED);
        final SocketOptions socketOptions = new SocketOptions();
        socketOptions.setKeepAlive(true);
        socketOptions.setConnectTimeoutMillis(15000);
        socketOptions.setReadTimeoutMillis(30000);

        Cluster cluster = Cluster.builder()
                .addContactPoint(host)
                .withPort(cqlPort)
                .withClusterName(clusterName)
                .withCompression(compression)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withRetryPolicy(retryPolicy)
                .withReconnectionPolicy(reconnectionPolicy)
                .withProtocolVersion(protocolVersion)
                .withSocketOptions(socketOptions)
                .withoutJMXReporting()
                .build();

        // Add Cluster for shutdown process
        ServerStarter.CASSANDRA_EMBEDDED.getShutdownHook().addCluster(cluster);

        return cluster;
    }

    private String extractAndValidateKeyspaceName(TypedMap parameters) {
        String keyspaceName = parameters.getTyped(DEFAULT_KEYSPACE_NAME);
        Validator.validateNotBlank(keyspaceName, "The provided keyspace name should not be blank");
        Validator.validateTrue(KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches(),
                "The provided keyspace name '%s' should match the " + "following pattern : '%s'", keyspaceName,
                KEYSPACE_NAME_PATTERN.pattern());
        return keyspaceName;
    }

    private void createKeyspaceIfNeeded(Session session, String keyspaceName, Boolean keyspaceDurableWrite) {
        StringBuilder createKeyspaceStatement = new StringBuilder("CREATE keyspace IF NOT EXISTS ");
        createKeyspaceStatement.append(keyspaceName);
        createKeyspaceStatement.append(" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
        if (!keyspaceDurableWrite) {
            createKeyspaceStatement.append(" AND DURABLE_WRITES=false");
        }
        final String query = createKeyspaceStatement.toString();

        LOGGER.info("Creating keyspace : " + query);

        session.execute(query);
    }

    private void executeStartupScripts(Session session, TypedMap parameters) {
        ScriptExecutor scriptExecutor = null;
        List<String> scriptLocations = parameters.getTypedOr(SCRIPT_LOCATIONS, new ArrayList<>());
        if (scriptLocations.size() > 0) {
            scriptExecutor = new ScriptExecutor(session);
            scriptLocations.forEach(scriptExecutor::executeScript);
        }

        final Map<String, Map<String, Object>> scriptTemplates = parameters.getTypedOr(SCRIPT_TEMPLATES, new HashMap<>());
        if (scriptTemplates.size() > 0) {
            scriptExecutor = scriptExecutor == null
                    ? new ScriptExecutor(session)
                    : scriptExecutor;

            final ScriptExecutor executor = scriptExecutor;

            scriptTemplates
                    .entrySet()
                    .forEach(entry -> executor.executeScriptTemplate(entry.getKey(), entry.getValue()));
        }
    }

    public Cluster getSingletonCluster() {
        return singletonCluster;
    }

    public Session getSingletonSession() {
        return singletonSession;
    }
}
