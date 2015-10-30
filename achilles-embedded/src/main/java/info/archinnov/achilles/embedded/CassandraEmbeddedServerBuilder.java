/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.validation.Validator;


/**
 * Builder for embedded Cassandra server

 * <pre class="code"><code class="java">

 * CassandraEmbeddedServerBuilder
 * .builder()
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


    private String dataFileFolder;

    private String commitLogFolder;

    private String savedCachesFolder;

    private boolean cleanDataFiles = false;

    private boolean cleanConfigFile = true;

    private int concurrentReads;

    private int concurrentWrites;

    private int cqlPort;

    private int thriftPort;

    private int storagePort;

    private int storageSSLPort;

    private String clusterName;

    private String keyspaceName;

    private boolean durableWrite = false;

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

        if (StringUtils.isNotBlank(dataFileFolder))
            cassandraParams.put(DATA_FILE_FOLDER, dataFileFolder);

        if (StringUtils.isNotBlank(commitLogFolder))
            cassandraParams.put(COMMIT_LOG_FOLDER, commitLogFolder);

        if (StringUtils.isNotBlank(savedCachesFolder))
            cassandraParams.put(SAVED_CACHES_FOLDER, savedCachesFolder);

        if (StringUtils.isNotBlank(clusterName))
            cassandraParams.put(CLUSTER_NAME, clusterName);

        if (StringUtils.isNotBlank(keyspaceName))
            cassandraParams.put(DEFAULT_KEYSPACE_NAME, keyspaceName);

        if (cqlPort > 0)
            cassandraParams.put(CASSANDRA_CQL_PORT, cqlPort);

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

        cassandraParams.put(KEYSPACE_DURABLE_WRITE, durableWrite);

        TypedMap parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(cassandraParams);
        return parameters;
    }
}
