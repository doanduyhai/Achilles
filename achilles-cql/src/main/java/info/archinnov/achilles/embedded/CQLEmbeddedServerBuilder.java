package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_SSL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_CONFIG_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLUSTER_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.COMMIT_LOG_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CONFIG_YAML_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DATA_FILE_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.entity.manager.CQLPersistenceManagerFactory;

public class CQLEmbeddedServerBuilder {

    private String entityPackages;

    private String dataFileFolder;

    private String commitLogFolder;

    private String savedCachesFolder;

    private String configYamlFile;

    private boolean cleanDataFiles = false;

    private boolean cleanConfigFile = true;

    private int cqlPort;

    private int storagePort;

    private int storageSSLPort;

    private String clusterName;

    private String keyspaceName;

    private CQLEmbeddedServerBuilder() {
    }

    private CQLEmbeddedServerBuilder(String entityPackages) {
        this.entityPackages = entityPackages;
    }

    public static CQLEmbeddedServerBuilder withEntityPackages(String entityPackages) {
        return new CQLEmbeddedServerBuilder(entityPackages);
    }

    public static CQLEmbeddedServerBuilder noEntityPackages() {
        return new CQLEmbeddedServerBuilder();
    }

    public CQLEmbeddedServerBuilder withDataFolder(String dataFolder) {
        this.dataFileFolder = dataFolder;
        return this;
    }

    public CQLEmbeddedServerBuilder withCommitLogFolder(String commitLogFolder) {
        this.commitLogFolder = commitLogFolder;
        return this;
    }

    public CQLEmbeddedServerBuilder withSavedCachesFolder(String savedCachesFolder) {
        this.savedCachesFolder = savedCachesFolder;
        return this;
    }

    public CQLEmbeddedServerBuilder withConfigYamlFile(String configYamlFile) {
        this.configYamlFile = configYamlFile;
        this.cleanConfigFile = false;
        return this;
    }

    public CQLEmbeddedServerBuilder cleanDataFilesAtStartup() {
        this.cleanDataFiles = true;
        return this;
    }

    public CQLEmbeddedServerBuilder withClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public CQLEmbeddedServerBuilder withKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    public CQLEmbeddedServerBuilder withCQLPort(int clqPort) {
        this.cqlPort = clqPort;
        return this;
    }

    public CQLEmbeddedServerBuilder withStoragePort(int storagePort) {
        this.storagePort = storagePort;
        return this;
    }

    public CQLEmbeddedServerBuilder withStorageSSLPort(int storageSSLPort) {
        this.storageSSLPort = storageSSLPort;
        return this;
    }

    public CQLPersistenceManagerFactory buildPersistenceManagerFactory() {

        final CQLEmbeddedServer embeddedServer = new CQLEmbeddedServer(buildConfigMap());
        return embeddedServer.getPersistenceManagerFactory();
    }

    public CQLPersistenceManager buildPersistenceManager() {
        final CQLEmbeddedServer embeddedServer = new CQLEmbeddedServer(buildConfigMap());
        return embeddedServer.getPersistenceManager();
    }

    private Map<String, Object> buildConfigMap() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(CLEAN_CASSANDRA_DATA_FILES, cleanDataFiles);
        config.put(CLEAN_CASSANDRA_CONFIG_FILE, cleanConfigFile);

        if(StringUtils.isNotBlank(entityPackages))
            config.put(ENTITY_PACKAGES,entityPackages);

        if (StringUtils.isNotBlank(dataFileFolder))
            config.put(DATA_FILE_FOLDER, dataFileFolder);

        if (StringUtils.isNotBlank(commitLogFolder))
            config.put(COMMIT_LOG_FOLDER, commitLogFolder);

        if (StringUtils.isNotBlank(savedCachesFolder))
            config.put(SAVED_CACHES_FOLDER, savedCachesFolder);

        if (StringUtils.isNotBlank(configYamlFile))
            config.put(CONFIG_YAML_FILE, configYamlFile);

        if (StringUtils.isNotBlank(clusterName))
            config.put(CLUSTER_NAME, clusterName);

        if (StringUtils.isNotBlank(keyspaceName))
            config.put(KEYSPACE_NAME, keyspaceName);

        if (cqlPort > 0)
            config.put(CASSANDRA_CQL_PORT, cqlPort);

        if (storagePort > 0)
            config.put(CASSANDRA_STORAGE_PORT, storagePort);

        if (storageSSLPort > 0)
            config.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        return config;
    }
}
