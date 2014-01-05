package info.archinnov.achilles.embedded;

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
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory;
import info.archinnov.achilles.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Session;

public class CassandraEmbeddedServerBuilder {

	private String entityPackages;

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
	private List<Interceptor<?>> eventsInterceptor = new ArrayList<Interceptor<?>>();

	private CassandraEmbeddedServerBuilder() {
	}

	private CassandraEmbeddedServerBuilder(String entityPackages) {
		this.entityPackages = entityPackages;
	}

	/**
	 * Bootstrap Achilles with entity packages
	 * 
	 * @param entityPackages
	 *            entity packages to scan for @Entity annotation
	 */
	public static CassandraEmbeddedServerBuilder withEntityPackages(String entityPackages) {
		return new CassandraEmbeddedServerBuilder(entityPackages);
	}

	/**
	 * Bootstrap Achilles without entity packages. Only nativeQuery() and
	 * nativeSession() are useful in this mode
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
	 */
	public CassandraEmbeddedServerBuilder withDataFolder(String dataFolder) {
		this.dataFileFolder = dataFolder;
		return this;
	}

	public CassandraEmbeddedServerBuilder withEventInterceptors(List<? extends Interceptor<?>> eventInterceptors) {
		this.eventsInterceptor.addAll(eventInterceptors);
		return this;
	}

	/**
	 * Specify commit log folder for the embedded Cassandra server. Default
	 * value is 'target/cassandra_embedded/commitlog'
	 * 
	 * @param commitLogFolder
	 *            commit log folder for the embedded Cassandra server
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
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
	 * @return
	 */
	public CassandraEmbeddedServerBuilder withDurableWrite(boolean durableWrite) {
		this.durableWrite = durableWrite;
		return this;
	}

	/**
	 * Build CQL Persistence Manager Factory
	 * 
	 * @return PersistenceManagerFactory
	 */
	public PersistenceManagerFactory buildPersistenceManagerFactory() {

		Map<String, Object> parameters = buildConfigMap();
		String keyspace = (String) parameters.get(KEYSPACE_NAME);
		final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters);
		return embeddedServer.getPersistenceManagerFactory(keyspace);
	}

	/**
	 * Build CQL Persistence Manager
	 * 
	 * @return PersistenceManager
	 */
	public PersistenceManager buildPersistenceManager() {
		Map<String, Object> parameters = buildConfigMap();
		String keyspace = (String) parameters.get(KEYSPACE_NAME);
		final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters);
		return embeddedServer.getPersistenceManager(keyspace);
	}

	/**
	 * Start an embedded Cassandra server but DO NOT bootstrap Achilles
	 * 
	 * @return native Java driver core Session
	 */
	public Session buildNativeSessionOnly() {
		this.buildNativeSessionOnly = true;
		Map<String, Object> parameters = buildConfigMap();
		String keyspace = (String) parameters.get(KEYSPACE_NAME);
		final CassandraEmbeddedServer embeddedServer = new CassandraEmbeddedServer(parameters);
		return embeddedServer.getNativeSession(keyspace);
	}

	private Map<String, Object> buildConfigMap() {
		Map<String, Object> config = new HashMap();
		config.put(CLEAN_CASSANDRA_DATA_FILES, cleanDataFiles);
		config.put(CLEAN_CASSANDRA_CONFIG_FILE, cleanConfigFile);

		if (StringUtils.isNotBlank(entityPackages))
			config.put(ENTITY_PACKAGES, entityPackages);

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

		if (thriftPort > 0)
			config.put(CASSANDRA_THRIFT_PORT, thriftPort);

		if (storagePort > 0)
			config.put(CASSANDRA_STORAGE_PORT, storagePort);

		if (storageSSLPort > 0)
			config.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);
		if (eventsInterceptor.size() > 0) {
			config.put(EVENT_INTERCEPTORS, eventsInterceptor);
		}

		config.put(KEYSPACE_DURABLE_WRITE, durableWrite);
		config.put(BUILD_NATIVE_SESSION_ONLY, buildNativeSessionOnly);

		Map<String, Object> parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(config);
		return parameters;
	}
}
