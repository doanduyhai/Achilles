package info.archinnov.achilles.entity.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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

	@Captor
	private ArgumentCaptor<Map<PropertyMeta<?, ?>, Class<?>>> mapCaptor;

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

		Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>> pair1 = new Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>>(
				entityMeta1, joinPropertyMetaToBeFilled);
		Map<PropertyMeta<?, ?>, Class<?>> map = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		map.put(longPropertyMeta, Long.class);

		Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>> pair2 = new Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>>(
				entityMeta2, map);

		when(entityParser.parseEntity(eq(keyspace), eq(Long.class))).thenReturn(pair1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class))).thenReturn(pair2);

		factory.bootstrap();

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);
		verify(entityParser).fillJoinEntityMeta(eq(keyspace), mapCaptor.capture(),
				eq(entityMetaMap));
		verify(columnFamilyHelper).validateOrCreateColumnFamilies(eq(entityMetaMap), anyBoolean());

		assertThat((Class) mapCaptor.getValue().get(longPropertyMeta)).isEqualTo(Long.class);
	}

	@Test
	public void should_bootstrap_no_join_property() throws Exception
	{
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);

		Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>> pair1 = new Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>>(
				entityMeta1, joinPropertyMetaToBeFilled);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class))).thenReturn(pair1);

		factory.bootstrap();

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityParser, never()).fillJoinEntityMeta(eq(keyspace), anyMap(), eq(entityMetaMap));
	}

	@Test
	public void should_exception_when_no_entity_found() throws Exception
	{
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(new ArrayList<Class<?>>());

		exception.expect(BeanMappingException.class);
		exception
				.expectMessage("No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages null");

		factory.bootstrap();
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
