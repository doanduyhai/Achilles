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

import java.util.Set;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.policies.Policies;

import info.archinnov.achilles.type.TypedMap;

public class CassandraEmbeddedConfigParameters {

    /**
     * Configuration parameters
     */

    public static final String USE_UNSAFE_CASSANDRA_DAEMON = "useUnsafeCassandraDaemon";

    public static final String CLEAN_CASSANDRA_DATA_FILES = "cleanCassandraDataFiles";

    public static final String CLEAN_CASSANDRA_CONFIG_FILE = "cleanCassandraConfigFile";

    public static final String LISTEN_ADDRESS = "listenAddress";
    public static final String RPC_ADDRESS = "rpcAddress";
    public static final String BROADCAST_ADDRESS = "broadcastAddress";
    public static final String BROADCAST_RPC_ADDRESS = "broadcastRpcAddress";

    public static final String SHUTDOWN_HOOK = "shutdownHook";

    public static final String DATA_FILE_FOLDER = "datafileFolder";

    public static final String COMMIT_LOG_FOLDER = "commitlogFolder";

    public static final String SAVED_CACHES_FOLDER = "savedCachesFolder";

    public static final String HINTS_FOLDER = "hintsFolder";

    public static final String CDC_RAW_FOLDER = "cdcRawFolder";

    public static final String LOGBACK_FILE = "logbackXmlFile";

    public static final String CLUSTER_NAME = "clusterName";

    public static final String COMPRESSION_TYPE = "compressionType";

    public static final String LOAD_BALANCING_POLICY = "loadBalancingPolicy";

    public static final String RETRY_POLICY = "retryPolicy";

    public static final String RECONNECTION_POLICY = "reconnectionPolicy";

    public static final String CASSANDRA_THRIFT_PORT = "thriftPort";

    public static final String CASSANDRA_CQL_PORT = "cqlPort";

    public static final String CASSANDRA_CONNECTION_PROTOCOL_VERSION = "connectionProtocolVersion";

    public static final String CASSANDRA_STORAGE_PORT = "storagePort";

    public static final String CASSANDRA_STORAGE_SSL_PORT = "storageSSLPort";

    public static final String CASSANDRA_JMX_PORT = "jmxPort";

    public static final String CASSANDRA_CONCURRENT_READS = "concurrentReads";

    public static final String CASSANDRA_CONCURRENT_WRITES = "concurrentWrites";

    public static final String DEFAULT_KEYSPACE_NAME = "defaultKeyspaceName";

    public static final String KEYSPACE_DURABLE_WRITE = "keyspaceDurableWrite";

    public static final String SCRIPT_LOCATIONS = "scriptLocations";
    public static final String SCRIPT_TEMPLATES = "scriptTemplates";

    /*
     * Default values
     */
    public static final String DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME = "achilles_embedded";
    static final String DEFAULT_ACHILLES_TEST_DATA_FOLDER = "target/cassandra_embedded/data";
    static final String DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER = "target/cassandra_embedded/commitlog";
    static final String DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER = "target/cassandra_embedded/saved_caches";
    static final String DEFAULT_ACHILLES_TEST_TRIGGERS_FOLDER = "target/cassandra_embedded/cassandra_triggers";
    static final String DEFAULT_ACHILLES_TEST_HINTS_FOLDER = "target/cassandra_embedded/hints";
    static final String DEFAULT_ACHILLES_TEST_CDC_RAW_FOLDER = "target/cassandra_embedded/cdc_raw";
    static final Set<String> DEFAULT_ACHILLES_TEST_FOLDERS = SetUtils.of(DEFAULT_ACHILLES_TEST_DATA_FOLDER,
            DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER, DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER,
            DEFAULT_ACHILLES_TEST_HINTS_FOLDER, DEFAULT_ACHILLES_TEST_CDC_RAW_FOLDER);
    static final String DEFAULT_CASSANDRA_EMBEDDED_LOGBACK_FILE = "target/cassandra_embedded/logback.xml";
    static final String DEFAULT_CASSANDRA_EMBEDDED_CLUSTER_NAME = "Achilles Embedded Cassandra Cluster";
    static final Boolean DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_DURABLE_WRITE = false;
    static final String DEFAULT_CASSANDRA_EMBEDDED_LISTEN_ADDRESS = "localhost";
    static final String DEFAULT_CASSANDRA_EMBEDDED_RPC_ADDRESS = "localhost";
    static final String DEFAULT_CASSANDRA_EMBEDDED_BROADCAST_ADDRESS = "localhost";
    static final String DEFAULT_CASSANDRA_EMBEDDED_BROADCAST_RPC_ADDRESS = "localhost";

    /**
     * Default values
     */

    static TypedMap mergeWithDefaultParameters(TypedMap parameters) {
        TypedMap defaultParams = new TypedMap();

        defaultParams.put(LISTEN_ADDRESS, DEFAULT_CASSANDRA_EMBEDDED_LISTEN_ADDRESS);
        defaultParams.put(RPC_ADDRESS, DEFAULT_CASSANDRA_EMBEDDED_RPC_ADDRESS);
        defaultParams.put(BROADCAST_ADDRESS, DEFAULT_CASSANDRA_EMBEDDED_BROADCAST_ADDRESS);
        defaultParams.put(BROADCAST_RPC_ADDRESS, DEFAULT_CASSANDRA_EMBEDDED_BROADCAST_RPC_ADDRESS);

        defaultParams.put(USE_UNSAFE_CASSANDRA_DAEMON, false);
        defaultParams.put(CLEAN_CASSANDRA_DATA_FILES, true);
        defaultParams.put(CLEAN_CASSANDRA_CONFIG_FILE, true);
        defaultParams.put(DATA_FILE_FOLDER, DEFAULT_ACHILLES_TEST_DATA_FOLDER);
        defaultParams.put(COMMIT_LOG_FOLDER, DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER);
        defaultParams.put(SAVED_CACHES_FOLDER, DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER);
        defaultParams.put(HINTS_FOLDER, DEFAULT_ACHILLES_TEST_HINTS_FOLDER);
        defaultParams.put(CDC_RAW_FOLDER, DEFAULT_ACHILLES_TEST_CDC_RAW_FOLDER);
        defaultParams.put(LOGBACK_FILE, DEFAULT_CASSANDRA_EMBEDDED_LOGBACK_FILE);
        defaultParams.put(CLUSTER_NAME, DEFAULT_CASSANDRA_EMBEDDED_CLUSTER_NAME);
        defaultParams.put(DEFAULT_KEYSPACE_NAME, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME);
        defaultParams.put(KEYSPACE_DURABLE_WRITE, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_DURABLE_WRITE);
        defaultParams.put(COMPRESSION_TYPE, ProtocolOptions.Compression.NONE);
        defaultParams.put(LOAD_BALANCING_POLICY, Policies.defaultLoadBalancingPolicy());
        defaultParams.put(RETRY_POLICY, Policies.defaultRetryPolicy());
        defaultParams.put(RECONNECTION_POLICY, Policies.defaultReconnectionPolicy());
        defaultParams.putAll(parameters);

        return defaultParams;
    }

}
