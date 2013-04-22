package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;

import java.lang.reflect.Method;
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
public class ThriftColumnFamilyCreatorTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftColumnFamilyCreator creator;

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private KeyspaceDefinition keyspaceDefinition;

	@Mock
	private ThriftColumnFamilyHelper thriftColumnFamilyHelper;

	private Set<String> cfs = new HashSet<String>();

	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	private EntityMeta<?> meta;

	private PropertyMeta<Void, String> simplePropertyMeta;

	private final Method[] accessors = new Method[2];

	private PropertyMeta<Void, Long> idMeta;

	@Before
	public void setUp() throws Exception
	{
		accessors[0] = TestBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		accessors[1] = TestBean.class.getDeclaredMethod("setId", Long.class);
		idMeta = PropertyMetaFactory.factory(Void.class, Long.class).type(SIMPLE)
				.propertyName("id").accessors(accessors).build();

		cfs.clear();
		Whitebox.setInternalState(creator, "thriftColumnFamilyHelper", thriftColumnFamilyHelper);
		Whitebox.setInternalState(creator, "cfs", cfs);
	}

	@Test
	public void should_list_all_column_families_on_initialization() throws Exception
	{
		ArrayList<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);
		when(keyspaceDefinition.getCfDefs()).thenReturn(cfDefs);

		ThriftColumnFamilyCreator creator = new ThriftColumnFamilyCreator(cluster, keyspace);

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

		when(cluster.addColumnFamily(cfDef, true)).thenReturn("id");

		String id = this.creator.addColumnFamily(cfDef);

		assertThat(id).isEqualTo("id");
	}

	@Test
	public void should_create_column_family_for_entity() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		when(thriftColumnFamilyHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.createColumnFamily(meta);

		verify(thriftColumnFamilyHelper).buildEntityCF(meta, "keyspace");
		verify(cluster).addColumnFamily(cfDef, true);

	}

	@Test
	public void should_create_column_family_for_column_family_direct_mapping() throws Exception
	{
		prepareData();
		meta.setColumnFamilyDirectMapping(true);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		meta.setClassName("entity");
		when(
				thriftColumnFamilyHelper.buildDirectMappingCF("keyspace", simplePropertyMeta,
						Long.class, "testCF", "entity")).thenReturn(cfDef);

		creator.createColumnFamily(meta);

		verify(cluster).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_create_column_family_for_counter() throws Exception
	{

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);

		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(thriftColumnFamilyHelper.buildCounterCF("keyspace")).thenReturn(cfDef);

		creator.validateOrCreateColumnFamilies(new HashMap<Class<?>, EntityMeta<?>>(), true, true);

		verify(cluster).addColumnFamily(cfDef, true);
	}

	@Test
	public void should_validate_column_family() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

		creator.validateOrCreateColumnFamilies(entityMetaMap, true, false);
		verify(thriftColumnFamilyHelper).validateCFWithEntityMeta(cfDef, meta);
	}

	@Test
	public void should_validate_column_family_for_external_wide_map() throws Exception
	{
		PropertyMeta<Integer, String> externalWideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class) //
				.field("externalWideMap") //
				.externalCf("externalCF") //
				.idSerializer(LONG_SRZ)//
				.type(PropertyType.WIDE_MAP) //
				.build();

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs",
				Arrays.asList((ColumnFamilyDefinition) cfDef, externalCFDef));

		creator.validateOrCreateColumnFamilies(entityMetaMap, true, false);
		verify(thriftColumnFamilyHelper).validateCFWithPropertyMeta(externalCFDef,
				externalWideMapMeta, "externalCF");
		verify(thriftColumnFamilyHelper).validateCFWithEntityMeta(cfDef, meta);
	}

	@Test
	public void should_validate_counter_column_family() throws Exception
	{

		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
		when(cfDef.getName()).thenReturn(COUNTER_CF);
		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList(cfDef));

		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(thriftColumnFamilyHelper.buildCounterCF("keyspace")).thenReturn(cfDef);

		creator.validateOrCreateColumnFamilies(new HashMap<Class<?>, EntityMeta<?>>(), true, true);

		verify(thriftColumnFamilyHelper).validateCounterCF(cfDef);
	}

	@Test
	public void should_validate_then_create_column_family_when_not_matching() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF2");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));
		when(thriftColumnFamilyHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.validateOrCreateColumnFamilies(entityMetaMap, true, false);

		verify(cluster).addColumnFamily(cfDef, true);
		assertThat(cfs).containsOnly("testCF2");
	}

	@Test
	public void should_validate_then_create_column_family_when_null() throws Exception
	{
		prepareData();

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
		ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
		when(cfDef.getName()).thenReturn("mocked_cfDef");
		when(thriftColumnFamilyHelper.buildEntityCF(meta, "keyspace")).thenReturn(cfDef);

		creator.validateOrCreateColumnFamilies(entityMetaMap, true, false);

		verify(cluster).addColumnFamily(cfDef, true);
		assertThat(cfs).containsOnly("mocked_cfDef");
	}

	@Test
	public void should_validate_then_create_column_family_for_external_wide_map_when_null()
			throws Exception
	{
		PropertyMeta<Integer, String> externalWideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class) //
				.field("externalWideMap") //
				.externalCf("externalCF") //
				.idSerializer(LONG_SRZ)//
				.type(PropertyType.WIDE_MAP) //
				.build();

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));
		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");
		when(
				thriftColumnFamilyHelper.buildDirectMappingCF("keyspace", externalWideMapMeta,
						Long.class, "externalCF", meta.getClassName())).thenReturn(externalCFDef);

		creator.validateOrCreateColumnFamilies(entityMetaMap, true, false);

		verify(thriftColumnFamilyHelper).validateCFWithEntityMeta(cfDef, meta);
		verify(cluster).addColumnFamily(externalCFDef, true);
	}

	@Test
	public void should_exception_because_column_family_not_found() throws Exception
	{
		prepareData();
		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The required column family 'testCF' does not exist for entity 'TestBean'");

		creator.validateOrCreateColumnFamilies(entityMetaMap, false, false);
	}

	@Test
	public void should_exception_because_column_family_not_found_for_external_wide_map()
			throws Exception
	{
		PropertyMeta<Integer, String> externalWideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class) //
				.field("externalWideMap") //
				.externalCf("externalCF") //
				.idSerializer(LONG_SRZ)//
				.type(PropertyType.WIDE_MAP) //
				.build();

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The required column family 'externalCF' does not exist for field 'externalWideMap'");

		creator.validateOrCreateColumnFamilies(entityMetaMap, false, false);
	}

	@Test
	public void should_exception_when_no_column_family_for_counter() throws Exception
	{

		Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception.expectMessage("The required column family '" + COUNTER_CF + "' does not exist");

		creator.validateOrCreateColumnFamilies(new HashMap<Class<?>, EntityMeta<?>>(), false, true);

	}

	private void prepareData(PropertyMeta<?, ?>... extraPropertyMetas)
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		for (PropertyMeta<?, ?> propertyMeta : extraPropertyMetas)
		{
			propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
		}

		simplePropertyMeta = PropertyMetaFactory.factory(Void.class, String.class).type(SIMPLE)
				.propertyName("name").accessors(accessors).build();

		propertyMetas.put("name", simplePropertyMeta);

		meta = entityMetaBuilder(idMeta).keyspace(keyspace).className("TestBean")
				.columnFamilyName("testCF").serialVersionUID(1L).propertyMetas(propertyMetas)
				.build();
		entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		entityMetaMap.put(this.getClass(), meta);

		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);
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
