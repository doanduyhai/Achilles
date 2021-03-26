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

import java.util.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.validation.Validator;


/**
 * Builder for embedded Cassandra server

 * <pre class="code"><code class="java">

 * CassandraEmbeddedServerBuilder
 * .builder()
 * .withListenAdress("127.0.0.1")
 * .withRpcAdress("127.0.0.1")
 * .withBroadcastAdress("127.0.0.1")
 * .withBroadcastRPCAdress("127.0.0.1")
 * .withClusterName("Test Cluster")
 * .withDataFolder("/home/user/cassandra/data")
 * .withCommitLogFolder("/home/user/cassandra/commitlog")
 * .withSavedCachesFolder("/home/user/cassandra/saved_caches")
 * .cleanDataFilesAtStartup(true)
 * .withClusterName("Test Cluster")
 * .withKeyspaceName("achilles_test")
 * .withCQLPort(9042)
 * .withThriftPort(9160)
 * .withStoragePort(7990)
 * .withStorageSSLPort(7999)
 * .withDurableWrite(true)
 * .withScript("init_script.cql")
 * .buildNativeCluster();

 * </code></pre>
 */
public class CassandraEmbeddedServerBuilder {

    private String listenAddress;
    private String rpcAddress;
    private String broadcastAddress = "";
    private String broadcastRpcAddress = "";

    private CassandraShutDownHook cassandraShutDownHook;

    private String dataFileFolder;

    private String commitLogFolder;

    private String savedCachesFolder;

    private String hintsFolder;

    private String cdcRawFolder;

    private boolean cleanDataFiles = true;

    private boolean cleanConfigFile = true;

    private int concurrentReads;

    private int concurrentWrites;

    private int cqlPort;

    private ProtocolVersion protocolVersion;

    private int jmxPort;

    private int thriftPort;

    private int storagePort;

    private int storageSSLPort;

    private String clusterName;

    private String keyspaceName;

    private boolean durableWrite = false;

    private boolean useUnsafeCassandraDaemon = false;

    private List<String> scriptLocations = new ArrayList<>();

    private Map<String, Map<String, Object>> scriptTemplates = new HashMap<>();

    private TypedMap cassandraParams = new TypedMap();

    private CassandraEmbeddedServerBuilder() {
    }


    /**
     * Create a new CassandraEmbeddedServerBuilder
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public static CassandraEmbeddedServerBuilder builder() {
        return new CassandraEmbeddedServerBuilder();
    }

    /**
     * Specify the listen address. Default value = <strong>localhost</strong>
     *
     * @param listenAddress the listen address
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withListenAddress(String listenAddress) {
        this.listenAddress = listenAddress;
        return this;
    }

    /**
     * Specify the rpc address. Default value = <strong>localhost</strong>
     *
     * @param rpcAddress the RPC address
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withRpcAddress(String rpcAddress) {
        this.rpcAddress = rpcAddress;
        return this;
    }

    /**
     * Specify the broadcast address. Default value = <strong>localhost</strong>
     * <br/>
     * <br/>
     * Leaving this blank will set it to the same value as <strong>listen_address</strong>
     * @param broadcastAddress the address to broadcast to other Cassandra nodes.
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withBroadcastAddress(String broadcastAddress) {
        this.broadcastAddress = broadcastAddress;
        return this;
    }

    /**
     * Specify the broadcast RPC address. Default value = <strong>localhost</strong>
     * <br/>
     * <br/>
     * This cannot be set to <strong>0.0.0.0</strong>. If left blank, this will be set to the value of
     * <strong>rpc_address</strong>.
     * If <strong>rpc_address</strong> is set to <strong>0.0.0.0</strong>,
     * <strong>broadcast_rpc_address</strong> must be set
     * @param broadcastRpcAddress the RPC address to broadcast to drivers and other Cassandra nodes
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withBroadcastRpcAddress(String broadcastRpcAddress) {
        this.broadcastRpcAddress = broadcastRpcAddress;
        return this;
    }

    /**
     * Inject a shutdown hook to control when to shutdown the embedded Cassandra process.
     * <br/>
     * <pre class="code"><code class="java">
     *
     * CassandraShutDownHook shutdownHook = new CassandraShutDownHook();
     *
     * Session session = CassandraEmbeddedServerBuilder.builder()
     *   .withShutdownHook(shutdownHook)
     *   ...
     *   .buildNativeSession();
     *
     * ...
     *
     * shutdownHook.shutdownNow();
     * </code></pre>
     * <br/>
     * <strong>Please note that upon call on <em>shutdownNow()</em>, Achilles will trigger the shutdown of:</strong>
     * <ul>
     *     <li><strong>the embedded Cassandra server</strong></li>
     *     <li><strong>the associated Cluster object</strong></li>
     *     <li><strong>the associated Session object</strong></li>
     * </ul>
     * @param cassandraShutDownHook shutdown hook to control the shutdown of the embedded Cassandra process;
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withShutdownHook(CassandraShutDownHook cassandraShutDownHook) {
        this.cassandraShutDownHook = cassandraShutDownHook;
        return this;
    }

    /**
     * Specify data folder for the embedded Cassandra server. Default value is
     * 'target/cassandra_embedded/data'
     *
     * @param dataFolder data folder for the embedded Cassandra server
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withDataFolder(String dataFolder) {
        this.dataFileFolder = dataFolder;
        return this;
    }

    /**
     * Specify commit log folder for the embedded Cassandra server. Default
     * value is 'target/cassandra_embedded/commitlog'
     *
     * @param commitLogFolder commit log folder for the embedded Cassandra server
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withCommitLogFolder(String commitLogFolder) {
        this.commitLogFolder = commitLogFolder;
        return this;
    }

    /**
     * Specify saved caches folder for the embedded Cassandra server. Default
     * value is 'target/cassandra_embedded/saved_caches'
     *
     * @param savedCachesFolder saved caches folder for the embedded Cassandra server
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withSavedCachesFolder(String savedCachesFolder) {
        this.savedCachesFolder = savedCachesFolder;
        return this;
    }

    /**
     * Specify hints folder for the embedded Cassandra server. Default
     * value is 'target/cassandra_embedded/hints'
     *
     * @param hintsFolder hints folder for the embedded Cassandra server
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withHintsFolder(String hintsFolder) {
        this.hintsFolder = hintsFolder;
        return this;
    }

    /**
     * Specify cdc_raw folder for the embedded Cassandra server. Default
     * value is 'target/cassandra_embedded/cdc_raw'
     *
     * @param cdcRawFolder cdc_raw folder for the embedded Cassandra server
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withCdcRawFolder(String cdcRawFolder) {
        this.cdcRawFolder = cdcRawFolder;
        return this;
    }

    /**
     * Whether to clean all data files in data folder, commit log folder and
     * saved caches folder at startup or not. Default value = 'true'
     *
     * @param cleanDataFilesAtStartup whether to clean all data files at startup or not
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder cleanDataFilesAtStartup(boolean cleanDataFilesAtStartup) {
        this.cleanDataFiles = cleanDataFilesAtStartup;
        return this;
    }

    /**
     * Specify the cluster name for the embedded Cassandra server. Default value
     * is 'Achilles Embedded Cassandra Cluster'
     *
     * @param clusterName cluster name
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * Specify the keyspace name for the embedded Cassandra server. Default
     * value is 'achilles_embedded'

     * <br/>
     * <strong>If the keyspace does not exist, it will be created by Achilles</strong>
     *
     * @param keyspaceName keyspace name
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    /**
     * Specify the native transport port (CQL port) for the embedded Cassandra
     * server. If not set, the port will be randomized at runtime
     *
     * @param clqPort native transport port
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withCQLPort(int clqPort) {
        this.cqlPort = clqPort;
        return this;
    }

    /**
     * Specify the connection protocol version for the embedded Cassandra
     * server. If not set, the version will be the newest supported.
     *
     * @param protocolVersion connection protocol version
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withConnectionProtocol(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
        return this;
    }

    /**
     * Specify the JMX port for the embedded Cassandra
     * server. If not set, the port will be randomized at runtime
     *
     * @param jmxPort JMX port
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withJMXPort(int jmxPort) {
        this.jmxPort = jmxPort;
        return this;
    }


    /**
     * Specify the rpc port (Thrift port) for the embedded Cassandra server. If
     * not set, the port will be randomized at runtime
     *
     * @param thriftPort rpc port
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
        return this;
    }

    /**
     * Specify the storage port for the embedded Cassandra server. If not set,
     * the port will be randomized at runtime
     *
     * @param storagePort storage port
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withStoragePort(int storagePort) {
        this.storagePort = storagePort;
        return this;
    }

    /**
     * Specify the storage SSL port for the embedded Cassandra server. If not
     * set, the port will be randomized at runtime
     *
     * @param storageSSLPort storage SSL port
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withStorageSSLPort(int storageSSLPort) {
        this.storageSSLPort = storageSSLPort;
        return this;
    }

    /**
     * Specify the number threads for concurrent reads for the embedded Cassandra
     * server. If not set, 32
     *
     * @param concurrentReads the number threads for concurrent reads
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withConcurrentReads(int concurrentReads) {
        this.concurrentReads = concurrentReads;
        return this;
    }

    /**
     * Specify the number threads for concurrent writes for the embedded Cassandra
     * server. If not set, 32
     *
     * @param concurrentWrites the number threads for concurrent writes
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withConcurrentWrites(int concurrentWrites) {
        this.concurrentWrites = concurrentWrites;
        return this;
    }

    /**
     * Specify the 'durable write' property for the embedded Cassandra server.
     * Default value is 'false'. If not set, Cassandra will not write to commit
     * log.

     * For testing purpose, it is recommended to deactivate it to speed up tests
     *
     * @param durableWrite whether to activate 'durable write' or not
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withDurableWrite(boolean durableWrite) {
        this.durableWrite = durableWrite;
        return this;
    }

    /**
     * Load an CQL script in the class path and execute it upon initialization
     * of the embedded Cassandra server

     * <br/>

     * Call this method as many times as there are CQL scripts to be executed.
     * <br/>
     * Example:
     * <br/>
     * <pre class="code"><code class="java">

     * CassandraEmbeddedServerBuilder
     * .withScript("script1.cql")
     * .withScript("script2.cql")
     * ...
     * .build();
     * </code></pre>
     *
     * @param scriptLocation location of the CQL script in the <strong>class path</strong>
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withScript(String scriptLocation) {
        Validator.validateNotBlank(scriptLocation, "The script location should not be blank while executing CassandraEmbeddedServerBuilder.withScript()");
        scriptLocations.add(scriptLocation.trim());
        return this;
    }

    /**
     * Load an CQL script template in the class path, inject the values into the template
     * to produce the final script and execute it upon initialization
     * of the embedded Cassandra server

     * <br/>

     * Call this method as many times as there are CQL templates to be executed.
     * <br/>
     * Example:
     * <br/>
     * <pre class="code"><code class="java">

     * Map&lt;String, Object&gt; map1 = new HashMap&lt;&gt;();

     * map1.put("id", 100L);
     * map1.put("date", new Date());
     * ...

     * CassandraEmbeddedServerBuilder
     * .withScriptTemplate("script1.cql", map1)
     * .withScriptTemplate("script2.cql", map2)
     * ...
     * .build();
     * </code></pre>
     *
     * @param scriptTemplateLocation location of the CQL script in the <strong>class path</strong>
     * @param values                 values to inject into the template.
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withScriptTemplate(String scriptTemplateLocation, Map<String, Object> values) {
        Validator.validateNotBlank(scriptTemplateLocation, "The script template should not be blank while executing CassandraEmbeddedServerBuilder.withScriptTemplate()");
        Validator.validateNotEmpty(values, "The template values should not be empty while executing CassandraEmbeddedServerBuilder.withScriptTemplate()");
        scriptTemplates.put(scriptTemplateLocation.trim(), values);
        return this;
    }

    /**
     * Inject Cassandra parameters
     *
     * @param cassandraParams cassandra parameter
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withParams(TypedMap cassandraParams) {
        this.cassandraParams.putAll(cassandraParams);
        return this;
    }

    /**
     * Use an unsafe version of the Cassandra daemon. This version will:
     * <ul>
     *     <li>disable JMX</li>
     *     <li>disable legacy schema migration</li>
     *     <li>disable pre-3.0 hints migration</li>
     *     <li>disable pre-3.0 batch entries migration</li>
     *     <li>disable auto compaction on all keyspaces. <strong>Your test/dev data should fit in memory normally</strong></li>
     *     <li>disable metrics</li>
     *     <li>disable GCInspector</li>
     *     <li>disable native mlock system call</li>
     *     <li>disable Thrift server</li>
     *     <li>disable startup checks (Jemalloc, validLaunchDate, JMXPorts, JvmOptions, JnaInitialization, initSigarLibrary, dataDirs, SSTablesFormat, SystemKeyspaceState, Datacenter, Rack)</li>
     *     <li>disable materialized view rebuild. <strong>You should clean your data folder between each test anyway</strong></li>
     *     <li>disable the SizeEstimatesRecorder (estimate SSTable size, who cares for unit testing or dev ?)</li>
     * </ul>
     * @return
     */
    public CassandraEmbeddedServerBuilder useUnsafeCassandraDeamon() {
        this.useUnsafeCassandraDaemon = true;
        return this;
    }

    /**
     * Use an unsafe version of the Cassandra daemon. This version will:
     * <ul>
     *     <li>disable JMX</li>
     *     <li>disable legacy schema migration</li>
     *     <li>disable pre-3.0 hints migration</li>
     *     <li>disable pre-3.0 batch entries migration</li>
     *     <li>disable auto compaction on all keyspaces. <strong>Your test/dev data should fit in memory normally</strong></li>
     *     <li>disable metrics</li>
     *     <li>disable GCInspector</li>
     *     <li>disable native mlock system call</li>
     *     <li>disable Thrift server</li>
     *     <li>disable startup checks (Jemalloc, validLaunchDate, JMXPorts, JvmOptions, JnaInitialization, initSigarLibrary, dataDirs, SSTablesFormat, SystemKeyspaceState, Datacenter, Rack)</li>
     *     <li>disable materialized view rebuild. <strong>You should clean your data folder between each test anyway</strong></li>
     *     <li>disable the SizeEstimatesRecorder (estimate SSTable size, who cares for unit testing or dev ?)</li>
     * </ul>
     * @return
     */
    public CassandraEmbeddedServerBuilder useUnsafeCassandraDeamon(boolean useUnsafeCassandraDaemon) {
        this.useUnsafeCassandraDaemon = useUnsafeCassandraDaemon;
        return this;
    }

    /**
     * Start an embedded Cassandra server but DO NOT bootstrap Achilles
     *
     * @return native Java driver core Cluster
     */
    public Cluster buildNativeCluster() {
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(buildConfigMap());
        return embeddedServer.getNativeCluster();
    }

    /**
     * Start an embedded Cassandra server but DO NOT bootstrap Achilles
     *
     * @return native Java driver core Session
     */
    public Session buildNativeSession() {
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(buildConfigMap());
        return embeddedServer.getNativeSession();
    }

    public CassandraEmbeddedServer buildServer() {
        return new CassandraEmbeddedServer(buildConfigMap());
    }

    private TypedMap buildConfigMap() {


        cassandraParams.put(CLEAN_CASSANDRA_DATA_FILES, cleanDataFiles);
        cassandraParams.put(CLEAN_CASSANDRA_CONFIG_FILE, cleanConfigFile);

        if (isNotBlank(listenAddress))
            cassandraParams.put(LISTEN_ADDRESS, listenAddress);

        if (isNotBlank(rpcAddress))
            cassandraParams.put(RPC_ADDRESS, rpcAddress);

        if (broadcastAddress != null)
            cassandraParams.put(BROADCAST_ADDRESS, broadcastAddress);

        if (broadcastRpcAddress != null)
            cassandraParams.put(BROADCAST_RPC_ADDRESS, broadcastRpcAddress);

        if (cassandraShutDownHook != null)
            cassandraParams.put(SHUTDOWN_HOOK, cassandraShutDownHook);

        if (isNotBlank(dataFileFolder))
            cassandraParams.put(DATA_FILE_FOLDER, dataFileFolder);

        if (isNotBlank(commitLogFolder))
            cassandraParams.put(COMMIT_LOG_FOLDER, commitLogFolder);

        if (isNotBlank(savedCachesFolder))
            cassandraParams.put(SAVED_CACHES_FOLDER, savedCachesFolder);

        if (isNotBlank(hintsFolder))
            cassandraParams.put(HINTS_FOLDER, hintsFolder);

        if (isNotBlank(cdcRawFolder))
            cassandraParams.put(CDC_RAW_FOLDER, cdcRawFolder);

        if (isNotBlank(clusterName))
            cassandraParams.put(CLUSTER_NAME, clusterName);

        if (isNotBlank(keyspaceName))
            cassandraParams.put(DEFAULT_KEYSPACE_NAME, keyspaceName);

        if (cqlPort > 0)
            cassandraParams.put(CASSANDRA_CQL_PORT, cqlPort);

        if (jmxPort > 0)
            cassandraParams.put(CASSANDRA_JMX_PORT, jmxPort);

        if (thriftPort > 0)
            cassandraParams.put(CASSANDRA_THRIFT_PORT, thriftPort);

        if (storagePort > 0)
            cassandraParams.put(CASSANDRA_STORAGE_PORT, storagePort);

        if (storageSSLPort > 0)
            cassandraParams.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        if (concurrentReads > 0)
            cassandraParams.put(CASSANDRA_CONCURRENT_READS, concurrentReads);

        if (concurrentWrites > 0)
            cassandraParams.put(CASSANDRA_CONCURRENT_READS, concurrentWrites);

        if (scriptLocations.size() > 0) {
            final List<String> existingScriptLocations = cassandraParams.getTypedOr(SCRIPT_LOCATIONS, new ArrayList<>());
            existingScriptLocations.addAll(scriptLocations);
            cassandraParams.put(SCRIPT_LOCATIONS, existingScriptLocations);
        }

        if (scriptTemplates.size() > 0) {
            final Map<String, Map<String, Object>> existingScriptTemplates = cassandraParams.getTypedOr(SCRIPT_TEMPLATES, new HashMap<>());
            existingScriptTemplates.putAll(scriptTemplates);
            cassandraParams.put(SCRIPT_TEMPLATES, existingScriptTemplates);
        }

        if (useUnsafeCassandraDaemon) {
            cassandraParams.put(USE_UNSAFE_CASSANDRA_DAEMON, true);
        }

        cassandraParams.put(KEYSPACE_DURABLE_WRITE, durableWrite);

        if (!cassandraParams.containsKey(CASSANDRA_CONNECTION_PROTOCOL_VERSION) || cassandraParams.get(CASSANDRA_CONNECTION_PROTOCOL_VERSION) == null) {
            cassandraParams.put(CASSANDRA_CONNECTION_PROTOCOL_VERSION, Optional.ofNullable(protocolVersion).orElse(ProtocolVersion.V4));
        }

        TypedMap parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(cassandraParams);
        return parameters;
    }
}
