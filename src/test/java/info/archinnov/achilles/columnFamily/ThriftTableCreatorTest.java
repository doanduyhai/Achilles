package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.PropertyMetaTestBuilder;

/**
 * ColumnFamilyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftTableCreatorTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftTableCreator creator;

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private KeyspaceDefinition keyspaceDefinition;

	@Mock
	private ThriftTableHelper thriftTableHelper;

	private Set<String> columnFamilyNames = new HashSet<String>();

	private Map<Class<?>, EntityMeta> entityMetaMap;

	private EntityMeta meta;

	private PropertyMeta<Void, String> simplePropertyMeta;

	private PropertyMeta<Void, Long> idMeta;

	private AchillesConfigurationContext configContext = new AchillesConfigurationContext();

	@Before
	public void setUp() throws Exception
	{
		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.type(SIMPLE)
				.field("id")
				.accessors()
				.build();

		columnFamilyNames.clear();
		Whitebox.setInternalState(creator, "thriftTableHelper", thriftTableHelper);
		Whitebox.setInternalState(creator, "columnFamilyNames", columnFamilyNames);
		configContext.setForceColumnFamilyCreation(true);
		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);
	}

	@Test
	public void should_list_all_column_families_on_initialization() throws Exception
	{
		ArrayList<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);
		when(keyspaceDefinition.getCfDefs()).thenReturn(cfDefs);

		ThriftTableCreator creator = new ThriftTableCreator(cluster, keyspace);

		assertThat(Whitebox.getInternalState(creator, "cfDefs")).isSameAs(cfDefs);

	}

	@Test
	public void should_discover_column_family() throws Exception
	{

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");
		cfDef.setKeyValidationClass("keyValidationClass");
		cfDef.setComment("comment");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

		ColumnFamilyDefinition discoveredCfDef = creator.discoverColumnFamily("testCF");

		assertThat(discoveredCfDef).isNotNull();
		assertThat(discoveredCfDef.getName()).isEqualTo("testCF");
		assertThat(discoveredCfDef.getKeyValidationClass()).isEqualTo("keyValidationClass");
		assertThat(discoveredCfDef.getComment()).isEqualTo("comment");
	}

	@Test
	public void should_add_column_family() throws Exception
	{
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		creator.addColumnFamily(cfDef);

		verify(cluster).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_not_add_column_family_if_already_added() throws Exception
	{
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("name");
		columnFamilyNames.add("name");

		creator.addColumnFamily(cfDef);

		verify(cluster, never()).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_create_column_family_for_entity() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		when(thriftTableHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.createColumnFamily(meta);

		verify(thriftTableHelper).buildEntityCF(meta, "keyspace");
		verify(cluster).addColumnFamily(cfDef, true);

	}

	@Test
	public void should_create_column_family_for_wide_row() throws Exception
	{
		prepareData();
		meta.setWideRow(true);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		meta.setClassName("entity");
		when(
				thriftTableHelper.buildWideRowCF("keyspace", simplePropertyMeta, Long.class,
						"testCF", "entity")).thenReturn(cfDef);

		creator.createColumnFamily(meta);

		verify(cluster).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_create_column_family_for_counter() throws Exception
	{

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(thriftTableHelper.buildCounterCF("keyspace")).thenReturn(cfDef);

		creator.validateOrCreateCFForCounter(true);

		verify(cluster).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_validate_column_family() throws Exception
	{
		meta = new EntityMeta();
		meta.setTableName("testCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");
		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

		creator.validateOrCreateCFForEntity(meta, false);
		verify(thriftTableHelper).validateCFWithEntityMeta(cfDef, meta);
	}

	@Test
	public void should_validate_column_family_for_wide_map() throws Exception
	{
		PropertyMeta<Integer, String> wideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)
				.field("externalWideMap")
				.externalCf("externalCF")
				.type(PropertyType.WIDE_MAP)
				.build();

		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs",
				Arrays.asList((ColumnFamilyDefinition) cfDef, externalCFDef));

		creator.validateOrCreateCFForWideMap(wideMapMeta, Long.class, false, "externalCF",
				"TestBean");
		verify(thriftTableHelper).validateWideRowWithPropertyMeta(externalCFDef, wideMapMeta,
				"externalCF");
	}

	@Test
	public void should_validate_counter_column_family() throws Exception
	{

		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
		when(cfDef.getName()).thenReturn(COUNTER_CF);
		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList(cfDef));

		when(keyspace.getKeyspaceName()).thenReturn("keyspace");

		creator.validateOrCreateCFForCounter(true);

		verify(thriftTableHelper).validateCounterCF(cfDef);
	}

	@Test
	public void should_validate_then_create_column_family_when_not_matching() throws Exception
	{
		meta = new EntityMeta();
		meta.setTableName("testCF");
		meta.setWideRow(false);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF2");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));
		when(thriftTableHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.validateOrCreateCFForEntity(meta, true);

		verify(cluster).addColumnFamily(cfDef, true);
		assertThat(columnFamilyNames).containsOnly("testCF2");
	}

	@Test
	public void should_validate_then_create_column_family_when_null() throws Exception
	{
		meta = new EntityMeta();
		meta.setTableName("testCF");
		meta.setWideRow(false);

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
		when(cfDef.getName()).thenReturn("mocked_cfDef");
		when(thriftTableHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.validateOrCreateCFForEntity(meta, true);

		verify(cluster).addColumnFamily(cfDef, true);
		assertThat(columnFamilyNames).containsOnly("mocked_cfDef");
	}

	@Test
	public void should_create_column_family_for_wide_row_when_not_existing() throws Exception
	{
		PropertyMeta<Integer, String> wideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)
				.field("externalWideMap")
				.externalCf("externalCF")
				.type(PropertyType.WIDE_MAP)
				.idClass(Long.class)
				.build();

		prepareData(wideMapMeta);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));
		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");
		when(
				thriftTableHelper.buildWideRowCF("keyspace", wideMapMeta, Long.class, "externalCF",
						meta.getClassName())).thenReturn(externalCFDef);

		creator.validateOrCreateCFForWideMap(wideMapMeta, Long.class, true, "externalCF",
				"TestBean");

		verify(cluster).addColumnFamily(externalCFDef, true);
	}

	@Test
	public void should_exception_because_column_family_not_found() throws Exception
	{
		prepareData();
		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		configContext.setForceColumnFamilyCreation(false);
		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The required column family 'testCF' does not exist for entity 'TestBean'");

		creator.validateOrCreateCFForEntity(meta, false);
	}

	@Test
	public void should_exception_because_wide_row_not_found() throws Exception
	{
		PropertyMeta<Integer, String> wideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)
				.field("externalWideMap")
				.externalCf("externalCF")
				.type(PropertyType.WIDE_MAP)
				.idClass(Long.class)
				.entityClassName("testEntity")
				.build();

		prepareData();
		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		configContext.setForceColumnFamilyCreation(false);
		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The required column family 'externalCF' does not exist for field 'externalWideMap' of entity 'testEntity'");

		creator
				.validateOrCreateCFForWideMap(wideMapMeta, Long.class, false, "externalCF",
						"testCF");
	}

	@Ignore
	@Test
	public void should_exception_because_column_family_not_found_for_external_wide_map()
			throws Exception
	{
		PropertyMeta<Integer, String> externalWideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)
				.field("externalWideMap")
				.externalCf("externalCF")
				.idClass(Long.class)
				.type(PropertyType.WIDE_MAP)
				.build();

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);
		configContext.setForceColumnFamilyCreation(false);
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The required column family 'externalCF' does not exist for field 'externalWideMap'");

		creator.validateOrCreateColumnFamilies(entityMetaMap, configContext, false);
	}

	@Test
	public void should_exception_when_no_column_family_for_counter() throws Exception
	{

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		configContext.setForceColumnFamilyCreation(false);

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception.expectMessage("The required column family '" + COUNTER_CF + "' does not exist");

		creator.validateOrCreateColumnFamilies(new HashMap<Class<?>, EntityMeta>(), configContext,
				true);

	}

	private void prepareData(PropertyMeta<?, ?>... extraPropertyMetas) throws Exception
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		for (PropertyMeta<?, ?> propertyMeta : extraPropertyMetas)
		{
			propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
		}

		simplePropertyMeta = PropertyMetaTestBuilder
				.noClass(Void.class, String.class)
				.type(SIMPLE)
				.field("name")
				.build();

		propertyMetas.put("name", simplePropertyMeta);

		meta = entityMetaBuilder(idMeta)
				.className("TestBean")
				.columnFamilyName("testCF")
				.serialVersionUID(1L)
				.propertyMetas(propertyMetas)
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ONE))
				.build();

		entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		entityMetaMap.put(this.getClass(), meta);
	}

	class TestBean
	{

		private Long id;

		public Long getId()
		{
			return id;
		}

		public void setId(Long id)
		{
			this.id = id;
		}

	}
}
