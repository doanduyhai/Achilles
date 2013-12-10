package info.archinnov.achilles.embedded;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class CassandraEmbeddedConfigParameters {

	/**
	 * Configuration parameters
	 */
	public static final String CLEAN_CASSANDRA_DATA_FILES = "cleanCassandraDataFiles";

	public static final String CLEAN_CASSANDRA_CONFIG_FILE = "cleanCassandraConfigFile";

	public static final String DATA_FILE_FOLDER = "datafileFolder";

	public static final String COMMIT_LOG_FOLDER = "commitlogFolder";

	public static final String SAVED_CACHES_FOLDER = "savedCachesFolder";

	public static final String CONFIG_YAML_FILE = "configYamlFile";

	public static final String CASSANDRA_THRIFT_PORT = "thriftPort";

	public static final String CASSANDRA_CQL_PORT = "cqlPort";

	public static final String CASSANDRA_STORAGE_PORT = "storagePort";

	public static final String CASSANDRA_STORAGE_SSL_PORT = "storageSSLPort";

	public static final String CLUSTER_NAME = "clusterName";

	public static final String KEYSPACE_NAME = "keyspaceName";

	public static final String EVENT_INTERCEPTORS = "eventInterceptors";

	public static final String KEYSPACE_DURABLE_WRITE = "keyspaceDurableWrite";

	public static final String ENTITY_PACKAGES = "entityPackages";

	public static final String BUILD_NATIVE_SESSION_ONLY = "buildNativeSessionOnly";

	/*
	 * Default values
	 */
	public static final String DEFAULT_CASSANDRA_HOST = "localhost";

	public static final String DEFAULT_ACHILLES_TEST_KEYSPACE_NAME = "achilles_test";

	static final String DEFAULT_ACHILLES_TEST_DATA_FOLDER = "target/cassandra_embedded/data";

	static final String DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER = "target/cassandra_embedded/commitlog";

	static final String DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER = "target/cassandra_embedded/saved_caches";

	static final Set<String> DEFAULT_ACHILLES_TEST_FOLDERS = ImmutableSet.of(DEFAULT_ACHILLES_TEST_DATA_FOLDER,
			DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER, DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER);

	static final String DEFAULT_ACHILLES_TEST_CONFIG_YAML_FILE = "target/cassandra_embedded/cassandra.yaml";

	static final String DEFAULT_CASSANDRA_EMBEDDED_CLUSTER_NAME = "Achilles Embedded Cassandra Cluster";

	static final String DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME = "achilles_embedded";

	static final Boolean DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_DURABLE_WRITE = true;

	/**
	 * Default values
	 */

	static Map<String, Object> mergeWithDefaultParameters(Map<String, Object> parameters) {
		Map<String, Object> defaultParams = new HashMap<String, Object>();
		defaultParams.put(CLEAN_CASSANDRA_DATA_FILES, true);
		defaultParams.put(CLEAN_CASSANDRA_CONFIG_FILE, true);
		defaultParams.put(DATA_FILE_FOLDER, DEFAULT_ACHILLES_TEST_DATA_FOLDER);
		defaultParams.put(COMMIT_LOG_FOLDER, DEFAULT_ACHILLES_TEST_COMMIT_LOG_FOLDER);
		defaultParams.put(SAVED_CACHES_FOLDER, DEFAULT_ACHILLES_TEST_SAVED_CACHES_FOLDER);
		defaultParams.put(CONFIG_YAML_FILE, DEFAULT_ACHILLES_TEST_CONFIG_YAML_FILE);
		defaultParams.put(CLUSTER_NAME, DEFAULT_CASSANDRA_EMBEDDED_CLUSTER_NAME);
		defaultParams.put(KEYSPACE_NAME, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME);
		defaultParams.put(KEYSPACE_DURABLE_WRITE, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_DURABLE_WRITE);
		defaultParams.put(BUILD_NATIVE_SESSION_ONLY, false);
		defaultParams.putAll(parameters);

		return defaultParams;
	}

}
