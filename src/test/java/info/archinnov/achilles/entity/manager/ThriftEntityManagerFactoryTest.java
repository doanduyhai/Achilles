package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.columnFamily.ColumnFamilyCreator;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.validator.EntityParsingValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

/**
 * ThriftEntityManagerFactoryImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerFactoryTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityManagerFactory factory = new ThriftEntityManagerFactory();

	@Mock
	private EntityParsingValidator validator;

	@Mock
	private EntityParser entityParser;

	@Mock
	private EntityExplorer entityExplorer;

	@Mock
	private ColumnFamilyCreator columnFamilyCreator;

	@Mock
	private ArgumentExtractorForThriftEMF argumentExtractor;

	@Mock
	private List<String> entityPackages;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Captor
	private ArgumentCaptor<EntityParsingContext> contextCaptor;

	@Mock
	private EntityMeta<Object> entityMeta1;

	@Mock
	private EntityMeta<Object> entityMeta2;

	@Mock
	private PropertyMeta<Void, Long> longPropertyMeta;

	@SuppressWarnings("unchecked")
	@Test
	public void should_bootstrap() throws Exception
	{
		final List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);
		classes.add(String.class);

		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
		when(entityParser.parseEntity(contextCaptor.capture()))
				.thenReturn(entityMeta1, entityMeta2);

		Whitebox.setInternalState(factory, "entityMetaMap", entityMetaMap);
		factory.bootstrap();

		verify(validator).validateAtLeastOneEntity(classes, entityPackages);
		verify(entityMetaMap).put(Long.class, entityMeta1);
		verify(entityMetaMap).put(String.class, entityMeta2);
		verify(entityParser).fillJoinEntityMeta(contextCaptor.capture(), eq(entityMetaMap));
		verify(columnFamilyCreator).validateOrCreateColumnFamilies(eq(entityMetaMap), anyBoolean(),
				eq(false));

		List<EntityParsingContext> contexts = contextCaptor.getAllValues();

		assertThat((Class<Long>) contexts.get(0).getCurrentEntityClass()).isEqualTo(Long.class);
		assertThat((Class<String>) contexts.get(1).getCurrentEntityClass()).isEqualTo(String.class);
	}

	@Test
	public void should_exception_when_no_entity_found() throws Exception
	{
		ArrayList<Class<?>> entities = new ArrayList<Class<?>>();
		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		doThrow(new AchillesBeanMappingException()).when(validator).validateAtLeastOneEntity(
				entities, entityPackages);
		exception.expect(AchillesBeanMappingException.class);
		factory.discoverEntities();
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
		EntityManager em = factory.createEntityManager(new HashMap<Integer, String>());

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
	public void should_init_consistency_levels() throws Exception
	{
		ConsistencyLevel read = ConsistencyLevel.ONE, write = ConsistencyLevel.ALL;
		Map<String, HConsistencyLevel> readMap = ImmutableMap.of("cf1", HConsistencyLevel.TWO,
				"cf2", HConsistencyLevel.THREE);
		Map<String, HConsistencyLevel> writeMap = ImmutableMap.of("cf1",
				HConsistencyLevel.EACH_QUORUM, "cf2", HConsistencyLevel.LOCAL_QUORUM);

		Map<String, Object> configMap = new HashMap<String, Object>();
		when(argumentExtractor.initDefaultReadConsistencyLevel(configMap)).thenReturn(read);
		when(argumentExtractor.initDefaultWriteConsistencyLevel(configMap)).thenReturn(write);
		when(argumentExtractor.initReadConsistencyMap(configMap)).thenReturn(readMap);
		when(argumentExtractor.initWriteConsistencyMap(configMap)).thenReturn(writeMap);

		AchillesConfigurableConsistencyLevelPolicy actual = factory
				.initConsistencyLevelPolicy(configMap);

		assertThat(actual.getConsistencyLevelForRead("cf1")).isEqualTo(HConsistencyLevel.TWO);
		assertThat(actual.getConsistencyLevelForRead("cf2")).isEqualTo(HConsistencyLevel.THREE);
		assertThat(actual.getConsistencyLevelForWrite("cf1")).isEqualTo(
				HConsistencyLevel.EACH_QUORUM);
		assertThat(actual.getConsistencyLevelForWrite("cf2")).isEqualTo(
				HConsistencyLevel.LOCAL_QUORUM);

		assertThat(actual.getConsistencyLevelForRead("default")).isEqualTo(HConsistencyLevel.ONE);
		assertThat(actual.getConsistencyLevelForWrite("default")).isEqualTo(HConsistencyLevel.ALL);
	}
}
