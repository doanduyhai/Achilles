package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.exception.AchillesException;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ArgumentExtractorForThriftEMFTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftArgumentExtractorTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ThriftArgumentExtractor extractor = new ThriftArgumentExtractor();

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private Map<String, Object> configMap = new HashMap<String, Object>();

	@Before
	public void setUp()
	{
		configMap.clear();
	}

	@Test
	public void should_init_cluster() throws Exception
	{
		configMap.put(CLUSTER_PARAM, cluster);

		Cluster actual = extractor.initCluster(configMap);

		assertThat(actual).isSameAs(cluster);
	}

	@Test
	public void should_init_cluster_from_hostname_and_clustername() throws Exception
	{
		configMap.put(HOSTNAME_PARAM, "localhost:9160");
		configMap.put(CLUSTER_NAME_PARAM, "Test Cluster");

		Cluster actual = extractor.initCluster(configMap);

		assertThat(actual).isNotNull();
		assertThat(actual).isInstanceOf(Cluster.class);
		assertThat(actual.getName()).isEqualTo("Test Cluster");
	}

	@Test
	public void should_exception_when_cluster_and_hostname_not_set() throws Exception
	{
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Either '"
						+ CLUSTER_PARAM
						+ "' property or '"
						+ HOSTNAME_PARAM
						+ "'/'"
						+ CLUSTER_NAME_PARAM
						+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");
		extractor.initCluster(configMap);
	}

	@Test
	public void should_exception_when_cluster_and_clustername_not_set() throws Exception
	{
		configMap.put(HOSTNAME_PARAM, "localhost:9160");

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Either '"
						+ CLUSTER_PARAM
						+ "' property or '"
						+ HOSTNAME_PARAM
						+ "'/'"
						+ CLUSTER_NAME_PARAM
						+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");
		extractor.initCluster(configMap);
	}

	@Test
	public void should_init_keyspace() throws Exception
	{
		configMap.put(KEYSPACE_PARAM, keyspace);

		Keyspace actual = extractor.initKeyspace(null, policy, configMap);

		assertThat(actual).isSameAs(keyspace);
		verify(keyspace).setConsistencyLevelPolicy(policy);
	}

	@Test
	public void should_init_keyspace_from_keyspacename() throws Exception
	{
		configMap.put(KEYSPACE_NAME_PARAM, "achilles");

		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster",
				new CassandraHostConfigurator("localhost:9161"));

		Keyspace actual = extractor.initKeyspace(cluster, policy, configMap);

		assertThat(actual).isNotNull();
		assertThat(actual).isInstanceOf(Keyspace.class);
		assertThat(actual.getKeyspaceName()).isEqualTo("achilles");
	}

	@Test
	public void should_exception_when_keyspace_and_keyspacename_not_set() throws Exception
	{
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Either '"
						+ KEYSPACE_PARAM
						+ "' property or '"
						+ KEYSPACE_NAME_PARAM
						+ "' property should be provided for Achilles ThrifEntityManagerFactory bootstraping");
		extractor.initKeyspace(null, policy, configMap);
	}
}
