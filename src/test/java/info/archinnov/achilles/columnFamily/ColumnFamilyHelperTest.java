package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory;
import info.archinnov.achilles.exception.InvalidColumnFamilyException;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * ColumnFamilyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyHelperTest
{

	@InjectMocks
	ColumnFamilyHelper helper;

	@Mock
	Cluster cluster;

	@Mock
	Keyspace keyspace;

	@Mock
	KeyspaceDefinition keyspaceDefinition;

	@Mock
	private ColumnFamilyBuilder columnFamilyBuilder;

	@Mock
	private ColumnFamilyValidator columnFamilyValidator;

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

		ReflectionTestUtils.setField(helper, "columnFamilyBuilder", columnFamilyBuilder);
		ReflectionTestUtils.setField(helper, "columnFamilyValidator", columnFamilyValidator);
	}

	@Test
	public void should_discover_column_family() throws Exception
	{
		when(keyspace.getKeyspaceName()).thenReturn("keyspace");
		when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");
		cfDef.setKeyValidationClass("keyValidationClass");
		cfDef.setComment("comment");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));

		ColumnFamilyDefinition discoveredCfDef = helper.discoverColumnFamily("testCF");

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

		String id = this.helper.addColumnFamily(cfDef);

		assertThat(id).isEqualTo("id");
	}

	@Test
	public void should_create_column_family_for_entity() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		when(columnFamilyBuilder.buildDynamicCompositeCF(meta, "keyspace")).thenReturn(cfDef);

		helper.createColumnFamily(meta);

		verify(columnFamilyBuilder).buildDynamicCompositeCF(meta, "keyspace");
		verify(cluster).addColumnFamily(cfDef, true);

	}

	@Test
	public void should_create_column_family_for_widerow() throws Exception
	{
		prepareData();
		meta.setWideRow(true);
		idMeta.setValueClass(Long.class);

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		meta.setClassName("entity");
		when(
				columnFamilyBuilder.buildCompositeCF("keyspace", simplePropertyMeta, Long.class,
						"testCF", "entity")).thenReturn(cfDef);

		helper.createColumnFamily(meta);

		verify(cluster).addColumnFamily(cfDef, true);

	}

	@Test
	public void should_validate_column_family() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));

		helper.validateOrCreateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyValidator).validateCFWithEntityMeta(cfDef, meta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_column_family_for_external_wide_map() throws Exception
	{
		GenericCompositeDao<Long, String> externalWideMapDao = mock(GenericCompositeDao.class);
		PropertyMeta<Integer, String> externalWideMapMeta = new PropertyMeta<Integer, String>();
		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>(
				"externalCF", externalWideMapDao, LONG_SRZ);
		externalWideMapMeta.setExternalWideMapProperties(externalWideMapProperties);
		externalWideMapMeta.setPropertyName("externalWideMap");

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);
		when(externalWideMapDao.getColumnFamily()).thenReturn("externalCF");

		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) externalCFDef, cfDef));

		helper.validateOrCreateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyValidator).validateCFWithPropertyMeta(externalCFDef,
				externalWideMapMeta, "externalCF");
		verify(columnFamilyValidator).validateCFWithEntityMeta(cfDef, meta);
	}

	@Test
	public void should_validate_then_create_column_family_when_not_matching() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF2");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));

		helper.validateOrCreateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyBuilder).buildDynamicCompositeCF(meta, "keyspace");
	}

	@Test
	public void should_validate_then_create_column_family_when_null() throws Exception
	{
		prepareData();
		when(keyspaceDefinition.getCfDefs()).thenReturn(null);

		helper.validateOrCreateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyBuilder).buildDynamicCompositeCF(meta, "keyspace");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_then_create_column_family_for_external_wide_map_when_null()
			throws Exception
	{
		GenericCompositeDao<Long, String> externalWideMapDao = mock(GenericCompositeDao.class);
		PropertyMeta<Integer, String> externalWideMapMeta = new PropertyMeta<Integer, String>();
		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>(
				"externalCF", externalWideMapDao, LONG_SRZ);
		externalWideMapMeta.setExternalWideMapProperties(externalWideMapProperties);
		externalWideMapMeta.setPropertyName("externalWideMap");

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);
		when(externalWideMapDao.getColumnFamily()).thenReturn("externalCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));
		BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
		externalCFDef.setName("externalCF");
		when(
				columnFamilyBuilder.buildCompositeCF("keyspace", externalWideMapMeta, Long.class,
						"externalCF", meta.getClassName())).thenReturn(externalCFDef);

		helper.validateOrCreateColumnFamilies(entityMetaMap, true);

		verify(columnFamilyValidator).validateCFWithEntityMeta(cfDef, meta);
		verify(cluster).addColumnFamily(externalCFDef, true);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_because_column_family_not_found() throws Exception
	{
		prepareData();
		when(keyspaceDefinition.getCfDefs()).thenReturn(null);

		helper.validateOrCreateColumnFamilies(entityMetaMap, false);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_because_column_family_not_found_for_external_wide_map()
			throws Exception
	{
		GenericCompositeDao<Long, String> externalWideMapDao = mock(GenericCompositeDao.class);
		PropertyMeta<Integer, String> externalWideMapMeta = new PropertyMeta<Integer, String>();
		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>(
				"externalCF", externalWideMapDao, SerializerUtils.LONG_SRZ);
		externalWideMapMeta.setExternalWideMapProperties(externalWideMapProperties);
		externalWideMapMeta.setPropertyName("externalWideMap");

		prepareData(externalWideMapMeta);
		idMeta.setValueClass(Long.class);
		when(externalWideMapDao.getColumnFamily()).thenReturn("externalCF");

		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));

		helper.validateOrCreateColumnFamilies(entityMetaMap, false);
	}

	@Test
	public void should_normalize_canonical_classname() throws Exception
	{
		String canonicalName = "org.achilles.entity.ClassName";

		String normalized = ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_even_class_name_exceeeds_48_characters() throws Exception
	{
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(canonicalName);

	}

	private void prepareData(PropertyMeta<?, ?>... extraPropertyMetas)
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		for (PropertyMeta<?, ?> propertyMeta : extraPropertyMetas)
		{
			propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
		}

		simplePropertyMeta = (PropertyMeta<Void, String>) PropertyMetaFactory
				.factory(Void.class, String.class).type(SIMPLE).propertyName("name")
				.accessors(accessors).build();

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
