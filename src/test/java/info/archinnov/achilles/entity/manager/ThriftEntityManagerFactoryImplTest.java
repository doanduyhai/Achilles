package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.counterDaoTL;
import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.joinPropertyMetaToBeFilledTL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.columnFamily.ColumnFamilyCreator;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.entity.parser.EntityParsingContext;
import info.archinnov.achilles.exception.BeanMappingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

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
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

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
	private ColumnFamilyCreator columnFamilyCreator;

	@Mock
	private CounterDao counterDao;

	@Before
	public void setUp()
	{
		joinPropertyMetaToBeFilled.clear();
		counterDaoTL.set(counterDao);
		joinPropertyMetaToBeFilledTL.set(joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_bootstrap() throws Exception
	{
		final List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);

		when(entityExplorer.discoverEntities(entityPackages)).thenAnswer(
				new Answer<List<Class<?>>>()
				{
					@Override
					public List<Class<?>> answer(InvocationOnMock invocation) throws Throwable
					{
						joinPropertyMetaToBeFilledTL.get().put(longPropertyMeta, Long.class);
						return classes;
					}
				});

		Map<PropertyMeta<?, ?>, Class<?>> map = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		map.put(longPropertyMeta, Long.class);

		when(entityParser.parseEntity(eq(keyspace), eq(Long.class))).thenReturn(entityMeta1);
		when(entityParser.parseEntity(eq(keyspace), eq(String.class))).thenReturn(entityMeta2);

		factory.bootstrap();

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);
		verify(columnFamilyCreator).validateOrCreateColumnFamilies(eq(entityMetaMap), anyBoolean(),
				eq(false));
		verify(entityParser, never()).fillJoinEntityMeta(eq(keyspace), anyMap(), eq(entityMetaMap));
	}

	@Test
	public void should_bootstrap_with_join_property() throws Exception
	{
		List<Class<?>> classes = new ArrayList<Class<?>>();
		EntityParsingContext context = new EntityParsingContext();
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);
		classes.add(Long.class);

		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(eq(keyspace), eq(Long.class))).thenReturn(entityMeta1);
		context.setJoinPropertyMetaToBeFilled(joinPropertyMetaToBeFilled);
		factory.discoverEntities(context);

		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityParser)
				.fillJoinEntityMeta(keyspace, joinPropertyMetaToBeFilled, entityMetaMap);
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

}
