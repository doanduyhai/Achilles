package info.archinnov.achilles.entity.factory;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import mapping.entity.ColumnFamilyBean;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * ThriftEntityManagerFactoryImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
public class ThriftEntityManagerFactoryImplTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl();

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private List<String> entityPackages;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;

	@Mock
	private EntityMeta entityMeta1;

	@Mock
	private EntityMeta entityMeta2;

	@Mock
	private PropertyMeta<Void, Long> longPropertyMeta;

	@Mock
	private EntityParser entityParser;

	@Mock
	private EntityExplorer entityExplorer;

	@Mock
	private ColumnFamilyHelper columnFamilyHelper;

	@Test
	public void should_bootstrap() throws Exception
	{
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(
				entityMeta1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(
				entityMeta2);

		ReflectionTestUtils.setField(factory, "forceColumnFamilyCreation", true);
		factory.bootstrap();

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);
		verify(columnFamilyHelper).validateOrCreateColumnFamilies(entityMetaMap, true);

	}

	@Test
	public void should_exception_when_no_entity_found() throws Exception
	{
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(new ArrayList<Class<?>>());

		exception.expect(BeanMappingException.class);
		exception
				.expectMessage("No entity with javax.persistence.Entity annotation found in the packages null");

		factory.bootstrap();
	}

	@Test
	public void should_discover_join_entities() throws Exception
	{
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);

		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(
				entityMeta1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(
				entityMeta2);

		JoinProperties joinProperties = new JoinProperties();
		when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when(longPropertyMeta.type()).thenReturn(PropertyType.SIMPLE);

		when(entityMetaMap.containsKey(Long.class)).thenReturn(true);
		when(entityMetaMap.get(Long.class)).thenReturn(entityMeta1);

		factory.discoverEntities(joinPropertyMetaToBeFilled);

		assertThat(joinProperties.getEntityMeta()).isSameAs(entityMeta1);
		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);

	}

	@Test
	public void should_discover_external_join_entities() throws Exception
	{
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);
		when(longPropertyMeta.getValueSerializer()).thenReturn(LONG_SRZ);

		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(
				entityMeta1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(
				entityMeta2);

		JoinProperties joinProperties = new JoinProperties();
		when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when(longPropertyMeta.type()).thenReturn(EXTERNAL_JOIN_WIDE_MAP);

		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>(
				"externalCF", null, SerializerUtils.LONG_SRZ);
		when(longPropertyMeta.getExternalWideMapProperties()).thenReturn(
				(ExternalWideMapProperties) externalWideMapProperties);

		when(entityMetaMap.containsKey(Long.class)).thenReturn(true);
		when(entityMetaMap.get(Long.class)).thenReturn(entityMeta1);
		when(entityMeta1.getIdSerializer()).thenReturn(LONG_SRZ);

		factory.discoverEntities(joinPropertyMetaToBeFilled);

		assertThat(joinProperties.getEntityMeta()).isSameAs(entityMeta1);
		GenericCompositeDao<Long, ?> externalWideMapDao = externalWideMapProperties
				.getExternalWideMapDao();
		assertThat(externalWideMapDao).isNotNull();
		assertThat(externalWideMapDao.getColumnFamily()).isEqualTo("externalCF");

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);

	}

	@Test
	public void should_throw_exception_when_no_entity_meta_found_for_join_entity() throws Exception
	{
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);

		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(
				entityMeta1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(
				entityMeta2);

		JoinProperties joinProperties = new JoinProperties();
		when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);

		when(entityMetaMap.containsKey(Long.class)).thenReturn(false);

		exception.expect(BeanMappingException.class);
		exception.expectMessage("Cannot find mapping for join entity 'java.lang.Long'");

		factory.discoverEntities(joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_throw_exception_when_direct_column_family_mapping_used_for_join()
			throws Exception
	{
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		joinPropertyMetaToBeFilled.put(longPropertyMeta, ColumnFamilyBean.class);

		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(
				entityMeta1);

		JoinProperties joinProperties = new JoinProperties();
		when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when(entityMetaMap.containsKey(ColumnFamilyBean.class)).thenReturn(true);
		when(entityMetaMap.get(ColumnFamilyBean.class)).thenReturn(entityMeta1);
		when(entityMeta1.isColumnFamilyDirectMapping()).thenReturn(true);

		exception.expect(BeanMappingException.class);
		exception.expectMessage("The entity '" + ColumnFamilyBean.class.getCanonicalName()
				+ "' is a direct Column Family mapping and cannot be a join entity");

		factory.discoverEntities(joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_create_entity_manager() throws Exception
	{
		EntityManager em = factory.createEntityManager();

		assertThat(em).isNotNull();
	}

	@Test
	public void should_create_entity_manager_with_parameters() throws Exception
	{
		EntityManager em = factory.createEntityManager(new HashMap());

		assertThat(em).isNotNull();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_return_true_when_open_called() throws Exception
	{
		factory.isOpen();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_do_nothing_when_close_called() throws Exception
	{
		factory.close();
	}

	@Test
	public void should_create_object_mapper_factory_from_provided_object_mapper() throws Exception
	{
		ObjectMapper mapper = mock(ObjectMapper.class);

		ObjectMapperFactory mapperFactory = ThriftEntityManagerFactoryImpl
				.factoryFromMapper(mapper);

		assertThat(mapperFactory).isNotNull();
		assertThat(mapperFactory.getMapper(String.class)).isSameAs(mapper);
	}
}
