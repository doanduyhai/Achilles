package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.columnFamily.AchillesTableCreator;
import info.archinnov.achilles.configuration.AchillesArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.validator.EntityParsingValidator;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesEntityManagerFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesEntityManagerFactoryTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private AchillesEntityManagerFactory factory;

	@Mock
	private AchillesTableCreator tableCreator;

	@Mock
	private EntityExplorer entityExplorer;

	@Mock
	private EntityParsingValidator validator;

	@Mock
	private EntityParser entityParser;

	@Mock
	private AchillesArgumentExtractor extractor;

	private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

	private List<String> entityPackages = new ArrayList<String>();

	@Before
	public void setUp()
	{
		doCallRealMethod().when(factory).setTableCreator(any(AchillesTableCreator.class));
		doCallRealMethod().when(factory).setEntityMetaMap(
				(Map<Class<?>, EntityMeta>) any(Map.class));
		doCallRealMethod().when(factory).setEntityPackages((List<String>) any(List.class));
		doCallRealMethod().when(factory).setEntityParser(any(EntityParser.class));
		doCallRealMethod().when(factory).setEntityExplorer(any(EntityExplorer.class));
		doCallRealMethod().when(factory).setValidator(any(EntityParsingValidator.class));

		factory.setTableCreator(tableCreator);
		factory.setEntityMetaMap(entityMetaMap);
		factory.setEntityPackages(entityPackages);
		factory.setEntityParser(entityParser);
		factory.setEntityExplorer(entityExplorer);
		factory.setValidator(validator);
	}

	@Test
	public void should_bootstrap() throws Exception
	{

		when(factory.discoverEntities()).thenReturn(true);
		doCallRealMethod().when(factory).bootstrap();
		factory.bootstrap();
		verify(tableCreator).validateOrCreateColumnFamilies(eq(entityMetaMap),
				any(AchillesConfigurationContext.class), eq(true));
	}

	@Test
	public void should_exception_during_boostrap() throws Exception
	{
		when(factory.discoverEntities()).thenThrow(new RuntimeException("test"));
		doCallRealMethod().when(factory).bootstrap();

		exception.expect(AchillesException.class);
		exception.expectMessage("Exception during entity parsing : test");
		factory.bootstrap();

	}

	@Test
	public void should_discover_entities() throws Exception
	{
		List<Class<?>> entities = new ArrayList<Class<?>>();
		entities.add(Long.class);
		EntityMeta entityMeta = new EntityMeta();

		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		when(entityParser.parseEntity(any(EntityParsingContext.class))).thenReturn(entityMeta);

		doCallRealMethod().when(factory).discoverEntities();
		factory.discoverEntities();

		assertThat(entityMetaMap).containsKey(Long.class);
		assertThat(entityMetaMap).containsValue(entityMeta);
		verify(validator).validateAtLeastOneEntity(entities, entityPackages);
		verify(entityParser).fillJoinEntityMeta(any(EntityParsingContext.class), eq(entityMetaMap));

	}

	@Test
	public void should_parse_configuration() throws Exception
	{
		AchillesConsistencyLevelPolicy policy = mock(AchillesConsistencyLevelPolicy.class);
		ObjectMapperFactory mapperFactory = mock(ObjectMapperFactory.class);
		Map<String, Object> configurationMap = new HashMap<String, Object>();

		when(extractor.ensureConsistencyOnJoin(configurationMap)).thenReturn(true);
		when(extractor.initForceCFCreation(configurationMap)).thenReturn(true);
		when(factory.initConsistencyLevelPolicy(configurationMap, extractor)).thenReturn(policy);
		when(extractor.initObjectMapperFactory(configurationMap)).thenReturn(mapperFactory);

		doCallRealMethod().when(factory).parseConfiguration(configurationMap, extractor);

		AchillesConfigurationContext builtContext = factory.parseConfiguration(configurationMap,
				extractor);

		assertThat(builtContext).isNotNull();
		assertThat(builtContext.isEnsureJoinConsistency()).isTrue();
		assertThat(builtContext.isForceColumnFamilyCreation()).isTrue();
		assertThat(builtContext.getConsistencyPolicy()).isSameAs(policy);
		assertThat(builtContext.getObjectMapperFactory()).isSameAs(mapperFactory);
	}

	@Test
	public void should_exception_when_get_criteria_builder() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).getCriteriaBuilder();

		factory.getCriteriaBuilder();
	}

	@Test
	public void should_exception_when_get_metamodel() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).getMetamodel();

		factory.getMetamodel();
	}

	@Test
	public void should_exception_when_get_cache() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).getCache();

		factory.getCache();
	}

	@Test
	public void should_exception_when_get_persistence_unit_util() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).getPersistenceUnitUtil();

		factory.getPersistenceUnitUtil();
	}

	@Test
	public void should_exception_when_close() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).close();

		factory.close();
	}

	@Test
	public void should_exception_when_is_open() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This operation is not supported by Achilles");

		doCallRealMethod().when(factory).isOpen();

		factory.isOpen();
	}

	@Test
	public void should_return_empty_properties_map() throws Exception
	{
		doCallRealMethod().when(factory).getProperties();
		Map<String, Object> properties = factory.getProperties();

		assertThat(properties).isNotNull();
		assertThat(properties).isEmpty();
	}
}
