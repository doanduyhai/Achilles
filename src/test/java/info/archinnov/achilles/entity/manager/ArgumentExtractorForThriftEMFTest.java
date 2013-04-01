package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.CLUSTER_NAME_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.CLUSTER_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.FORCE_CF_CREATION_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.HOSTNAME_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.KEYSPACE_PARAM;
import static info.archinnov.achilles.entity.manager.ArgumentExtractorForThriftEMF.OBJECT_MAPPER_FACTORY_PARAM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

/**
 * ArgumentExtractorForThriftEMFTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ArgumentExtractorForThriftEMFTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ArgumentExtractorForThriftEMF extractor = new ArgumentExtractorForThriftEMF();

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private ObjectMapperFactory factory;

	@Mock
	private ObjectMapper mapper;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private Map<String, Object> configMap = new HashMap<String, Object>();

	@Before
	public void setUp()
	{
		configMap.clear();
	}

	@Test
	public void should_init_entity_packages() throws Exception
	{
		configMap.put(ENTITY_PACKAGES_PARAM,
				"my.package.entity,another.package.entity,third.package");

		List<String> actual = extractor.initEntityPackages(configMap);

		assertThat(actual).containsExactly("my.package.entity", "another.package.entity",
				"third.package");
	}

	@Test
	public void should_exception_when_entity_packages_not_set() throws Exception
	{
		exception.expect(AchillesException.class);
		exception.expectMessage("'" + ENTITY_PACKAGES_PARAM
				+ "' property should be set for Achilles ThrifEntityManagerFactory bootstraping");
		extractor.initEntityPackages(configMap);
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

	@Test
	public void should_init_forceCFCreation() throws Exception
	{
		configMap.put(FORCE_CF_CREATION_PARAM, true);

		boolean actual = extractor.initForceCFCreation(configMap);

		assertThat(actual).isTrue();

	}

	@Test
	public void should_init_forceCFCreation_to_default_value() throws Exception
	{
		boolean actual = extractor.initForceCFCreation(configMap);

		assertThat(actual).isFalse();
	}

	@Test
	public void should_init_object_mapper_factory() throws Exception
	{
		configMap.put(OBJECT_MAPPER_FACTORY_PARAM, factory);

		ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

		assertThat(actual).isSameAs(factory);
	}

	@Test
	public void should_init_object_mapper_factory_from_mapper() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.OBJECT_MAPPER_PARAM, mapper);

		ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

		assertThat(actual).isNotNull();
		assertThat(actual.getMapper(Long.class)).isSameAs(mapper);
	}

	@Test
	public void should_init_default_object_factory_mapper() throws Exception
	{
		ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

		assertThat(actual).isNotNull();

		ObjectMapper mapper = actual.getMapper(Integer.class);

		assertThat(mapper).isNotNull();
		assertThat(mapper.getSerializationConfig().getSerializationInclusion()).isEqualTo(
				Inclusion.NON_NULL);
		assertThat(
				mapper.getDeserializationConfig().isEnabled(
						DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES)).isFalse();
		Collection<AnnotationIntrospector> ais = mapper.getSerializationConfig()
				.getAnnotationIntrospector().allIntrospectors();

		assertThat(ais).hasSize(2);
		Iterator<AnnotationIntrospector> iterator = ais.iterator();

		assertThat(iterator.next()).isInstanceOfAny(JacksonAnnotationIntrospector.class,
				JaxbAnnotationIntrospector.class);
		assertThat(iterator.next()).isInstanceOfAny(JacksonAnnotationIntrospector.class,
				JaxbAnnotationIntrospector.class);
	}

	@Test
	public void should_init_default_read_consistency_level() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.DEFAUT_READ_CONSISTENCY_PARAM, "ONE");
		assertThat(extractor.initDefaultReadConsistencyLevel(configMap)).isEqualTo(ONE);
	}

	@Test
	public void should_init_default_write_consistency_level() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.DEFAUT_WRITE_CONSISTENCY_PARAM, "LOCAL_QUORUM");
		assertThat(extractor.initDefaultWriteConsistencyLevel(configMap)).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_return_default_quorum_level_when_no_parameter() throws Exception
	{
		assertThat(extractor.initDefaultReadConsistencyLevel(configMap)).isEqualTo(QUORUM);
	}

	@Test
	public void should_exception_when_invalid_consistency_lvel() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.DEFAUT_READ_CONSISTENCY_PARAM, "wrong_value");

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("'wrong_value' is not a valid Consistency Level");
		extractor.initDefaultReadConsistencyLevel(configMap);
	}

	@Test
	public void should_init_read_consistency_level_map() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.READ_CONSISTENCY_MAP_PARAM,
				ImmutableMap.of("cf1", "ONE", "cf2", "LOCAL_QUORUM"));

		Map<String, HConsistencyLevel> consistencyMap = extractor.initReadConsistencyMap(configMap);

		assertThat(consistencyMap.get("cf1")).isEqualTo(HConsistencyLevel.ONE);
		assertThat(consistencyMap.get("cf2")).isEqualTo(HConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_init_write_consistency_level_map() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.WRITE_CONSISTENCY_MAP_PARAM,
				ImmutableMap.of("cf1", "THREE", "cf2", "EACH_QUORUM"));

		Map<String, HConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap.get("cf1")).isEqualTo(HConsistencyLevel.THREE);
		assertThat(consistencyMap.get("cf2")).isEqualTo(HConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_return_empty_consistency_map_when_no_parameter() throws Exception
	{
		Map<String, HConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap).isEmpty();
	}

	@Test
	public void should_return_empty_consistency_map_when_empty_map_parameter() throws Exception
	{
		configMap.put(ArgumentExtractorForThriftEMF.WRITE_CONSISTENCY_MAP_PARAM,
				new HashMap<String, String>());

		Map<String, HConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap).isEmpty();
	}
}
