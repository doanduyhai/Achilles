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

import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.BUILD_NATIVE_SESSION_ONLY;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_SSL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_THRIFT_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_CONFIG_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLUSTER_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.COMMIT_LOG_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CONFIG_YAML_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DATA_FILE_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.type.TypedMap;

/**
 * Builder for embedded Cassandra server
 *
 * <pre class="code"><code class="java">
 *
 * CassandraEmbeddedServerBuilder
 *   .withEntityPackages("test")
 *   .withClusterName("Test Cluster")
 *   .withDataFolder("/home/user/cassandra/data")
 *   .withCommitLogFolder("/home/user/cassandra/commitlog")
 *   .withSavedCachesFolder("/home/user/cassandra/saved_caches")
 *   .cleanDataFilesAtStartup(true)
 *   .withConfigYamlFile("/home/user/cassandra/cassandra.yaml")
 *   .withClusterName("Test Cluster")
 *   .withKeyspaceName("achilles_test")
 *   .withCQLPort(9042)
 *   .withThriftPort(9160)
 *   .withStoragePort(7990)
 *   .withStorageSSLPort(7999)
 *   .withDurableWrite(true)
 *   .buildPersistenceManager();
 *
 * </code></pre>
 *
 */
public class CassandraEmbeddedServerBuilder {

    private String entityPackages;

    private List<Class<?>>
            entities = Collections.emptyList();

    private String dataFileFolder;

    private String commitLogFolder;

    private String savedCachesFolder;

    private String configYamlFile;

    private boolean cleanDataFiles = false;

    private boolean cleanConfigFile = true;

    private int cqlPort;

    private int thriftPort;

    private int storagePort;

    private int storageSSLPort;

    private String clusterName;

    private String keyspaceName;

    private boolean durableWrite = true;

    private boolean buildNativeSessionOnly = false;

    private boolean buildNativeClusterOnly = false;

    private Map<ConfigurationParameters, Object> achillesConfigParams = new HashMap<>();

    private CassandraEmbeddedServerBuilder() {
    }

    private CassandraEmbeddedServerBuilder(String entityPackages) {
        this.entityPackages = entityPackages;
    }

    private CassandraEmbeddedServerBuilder(List<Class<?>> entities) {
        this.entities = entities;
    }

    /**
     * Bootstrap Achilles with entity packages
     *
     * @param entityPackages
     *            entity packages to scan for @Entity annotation
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public static CassandraEmbeddedServerBuilder withEntityPackages(String entityPackages) {
        return new CassandraEmbeddedServerBuilder(entityPackages);
    }

    /**
     * Bootstrap Achilles with entities list
     *
     * @param entities
     *            entities list
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public static CassandraEmbeddedServerBuilder withEntities(List<Class<?>> entities) {
        return new CassandraEmbeddedServerBuilder(entities);
    }

    /**
     * Bootstrap Achilles with entities
     *
     * @param entities
     *            entities
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public static CassandraEmbeddedServerBuilder withEntities(Class<?>... entities) {
        return new CassandraEmbeddedServerBuilder(Arrays.asList(entities));
    }

    /**
     * Bootstrap Achilles without entity packages. Only nativeQuery(),
     * nativeSession() and nativeCluster() are useful in this mode
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public static CassandraEmbeddedServerBuilder noEntityPackages() {
        return new CassandraEmbeddedServerBuilder();
    }

    /**
     * Specify data folder for the embedded Cassandra server. Default value is
     * 'target/cassandra_embedded/data'
     *
     * @param dataFolder
     *            data folder for the embedded Cassandra server
     *
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
     * @param commitLogFolder
     *            commit log folder for the embedded Cassandra server
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
     * @param savedCachesFolder
     *            saved caches folder for the embedded Cassandra server
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withSavedCachesFolder(String savedCachesFolder) {
        this.savedCachesFolder = savedCachesFolder;
        return this;
    }

    /**
     * Specify path to 'cassandra.yaml' config file for the embedded Cassandra
     * server. If not set, a default 'cassandra.yaml' file will be created in
     * the 'target/cassandra_embedded' folder
     *
     * @param configYamlFile
     *            path to 'cassandra.yaml' config file for the embedded
     *            Cassandra server
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withConfigYamlFile(String configYamlFile) {
        this.configYamlFile = configYamlFile;
        this.cleanConfigFile = false;
        return this;
    }

    /**
     * Whether to clean all data files in data folder, commit log folder and
     * saved caches folder at startup or not. Default value = 'true'
     *
     * @param cleanDataFilesAtStartup
     *            whether to clean all data files at startup or not
     *
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
     * @param clusterName
     *            cluster name
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * Specify the keyspace name for the embedded Cassandra server. Default
     * value is 'achilles_embedded'
     *
     * @param keyspaceName
     *            keyspace name
     *
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
     * @param clqPort
     *            native transport port
     *
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
     * @param thriftPort
     *            rpc port
     *
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
     * @param storagePort
     *            storage port
     *
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
     * @param storageSSLPort
     *            storage SSL port
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withStorageSSLPort(int storageSSLPort) {
        this.storageSSLPort = storageSSLPort;
        return this;
    }

    /**
     * Specify the 'durable write' property for the embedded Cassandra server.
     * Default value is 'false'. If not set, Cassandra will not write to commit
     * log.
     *
     * For testing purpose, it is recommended to deactivate it to speed up tests
     *
     * @param durableWrite
     *            whether to activate 'durable write' or not
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withDurableWrite(boolean durableWrite) {
        this.durableWrite = durableWrite;
        return this;
    }

    /**
     * Add Achilles configuration parameters
     *
     * @param configParams
     *            Achilles configuration parameters
     *
     * @return CassandraEmbeddedServerBuilder
     */
    public CassandraEmbeddedServerBuilder withAchillesConfigParams(Map<ConfigurationParameters, Object> configParams) {
        if (configParams != null && configParams.size() > 0)
            this.achillesConfigParams.putAll(configParams);
        return this;
    }

    /**
     * Build CQL Persistence Manager Factory
     *
     * @return PersistenceManagerFactory
     */
    public PersistenceManagerFactory buildPersistenceManagerFactory() {
        TypedMap parameters = buildConfigMap();
        ConfigMap achillesParameters = buildAchillesConfigMap();
        String keyspace = achillesParameters.getTyped(KEYSPACE_NAME);
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters, achillesParameters);
        return embeddedServer.getPersistenceManagerFactory(keyspace);
    }

    /**
     * Build CQL Persistence Manager
     *
     * @return PersistenceManager
     */
    public PersistenceManager buildPersistenceManager() {
        TypedMap parameters = buildConfigMap();
        ConfigMap achillesParameters = buildAchillesConfigMap();
        String keyspace = achillesParameters.getTyped(KEYSPACE_NAME);
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters, achillesParameters);
        return embeddedServer.getPersistenceManager(keyspace);
    }

    /**
     * Start an embedded Cassandra server but DO NOT bootstrap Achilles
     *
     * @return native Java driver core Session
     */
    public Session buildNativeSessionOnly() {
        this.buildNativeSessionOnly = true;
        TypedMap parameters = buildConfigMap();
        ConfigMap achillesParameters = buildAchillesConfigMap();
        String keyspace = achillesParameters.getTyped(KEYSPACE_NAME);
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters, achillesParameters);
        return embeddedServer.getNativeSession(keyspace);
    }

    /**
     * Start an embedded Cassandra server but DO NOT bootstrap Achilles
     *
     * @return native Java driver core Cluster
     */
    public Cluster buildNativeClusterOnly() {
        this.buildNativeClusterOnly = true;
        TypedMap parameters = buildConfigMap();
        ConfigMap achillesParameters = buildAchillesConfigMap();
        final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters, achillesParameters);
        return embeddedServer.getNativeCluster();
    }


    private ConfigMap buildAchillesConfigMap() {
        ConfigMap config = new ConfigMap();
        if (StringUtils.isNotBlank(entityPackages))
            config.put(ENTITY_PACKAGES, entityPackages);
        if (CollectionUtils.isNotEmpty(entities)) {
            config.put(ENTITIES_LIST, entities);
        }
        if (StringUtils.isNotBlank(keyspaceName)) {
            config.put(KEYSPACE_NAME, keyspaceName);
        }

        config.putAll(achillesConfigParams);
        return config;
    }

    private TypedMap buildConfigMap() {
        TypedMap config = new TypedMap();
        config.put(CLEAN_CASSANDRA_DATA_FILES, cleanDataFiles);
        config.put(CLEAN_CASSANDRA_CONFIG_FILE, cleanConfigFile);

        if (StringUtils.isNotBlank(dataFileFolder))
            config.put(DATA_FILE_FOLDER, dataFileFolder);

        if (StringUtils.isNotBlank(commitLogFolder))
            config.put(COMMIT_LOG_FOLDER, commitLogFolder);

        if (StringUtils.isNotBlank(savedCachesFolder))
            config.put(SAVED_CACHES_FOLDER, savedCachesFolder);

        if (StringUtils.isNotBlank(clusterName))
            config.put(CLUSTER_NAME, clusterName);

        if (cqlPort > 0)
            config.put(CASSANDRA_CQL_PORT, cqlPort);

        if (thriftPort > 0)
            config.put(CASSANDRA_THRIFT_PORT, thriftPort);

        if (storagePort > 0)
            config.put(CASSANDRA_STORAGE_PORT, storagePort);

        if (storageSSLPort > 0)
            config.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        config.put(KEYSPACE_DURABLE_WRITE, durableWrite);

        config.put(BUILD_NATIVE_SESSION_ONLY, buildNativeSessionOnly);

        config.put(BUILD_NATIVE_CLUSTER_ONLY, buildNativeClusterOnly);

        TypedMap parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(config);
        return parameters;
    }
}
