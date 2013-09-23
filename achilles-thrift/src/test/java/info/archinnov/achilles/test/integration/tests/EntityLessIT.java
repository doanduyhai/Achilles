package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import static info.archinnov.achilles.embedded.AchillesEmbeddedServer.*;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.exception.AchillesException;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Test;

public class EntityLessIT {

	@Test(expected = AchillesException.class)
	public void should_exception_when_no_entity_package_provided()
			throws Exception {
		Cluster cluster = HFactory.getOrCreateCluster("Achilles-cluster",
				CASSANDRA_TEST_HOST + ":" + CASSANDRA_THRIFT_TEST_PORT);
		Keyspace keyspace = HFactory.createKeyspace("system", cluster);

		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put(CLUSTER_PARAM, cluster);
		configMap.put(KEYSPACE_PARAM, keyspace);

		new ThriftEntityManagerFactory(configMap);
	}
}
