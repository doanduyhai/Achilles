package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.metadata.factory.PropertyMetaFactory;
import fr.doan.achilles.exception.InvalidColumnFamilyException;

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
	public void should_create_entity_meta() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		when(columnFamilyBuilder.buildForEntity(meta, "keyspace")).thenReturn(cfDef);

		helper.createColumnFamily(meta);

		verify(columnFamilyBuilder).buildForEntity(meta, "keyspace");
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

		helper.validateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyValidator).validate(cfDef, meta);
	}

	@Test
	public void should_validate_then_create_column_family_when_not_matching() throws Exception
	{
		prepareData();
		BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
		cfDef.setName("testCF2");

		when(keyspaceDefinition.getCfDefs()).thenReturn(
				Arrays.asList((ColumnFamilyDefinition) cfDef));

		helper.validateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyBuilder).buildForEntity(meta, "keyspace");
	}

	@Test
	public void should_validate_then_create_column_family_when_null() throws Exception
	{
		prepareData();
		when(keyspaceDefinition.getCfDefs()).thenReturn(null);

		helper.validateColumnFamilies(entityMetaMap, true);
		verify(columnFamilyBuilder).buildForEntity(meta, "keyspace");
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_because_column_family_not_found() throws Exception
	{
		prepareData();
		when(keyspaceDefinition.getCfDefs()).thenReturn(null);

		helper.validateColumnFamilies(entityMetaMap, false);
	}

	@Test
	public void should_normalize_canonical_classname() throws Exception
	{
		String canonicalName = "fr.doan.achilles.entity.metadata.ClassName";

		String normalized = ColumnFamilyHelper.normalizeCanonicalName(canonicalName);

		assertThat(normalized).isEqualTo("fr_doan_achilles_entity_metadata_ClassName");
	}

	@Test
	public void should_normalize_canonical_classname_by_shortening_package() throws Exception
	{
		String canonicalName = "fr.doan.achilles.entity.metadata.very.long.package.name.ClassName";

		String normalized = ColumnFamilyHelper.normalizeCanonicalName(canonicalName);

		assertThat(normalized).isEqualTo("fr_do_ac_en_me_ve_lo_pa_na_ClassName");
	}

	@Test
	public void should_normalize_canonical_classname_when_exceeding_48_characters()
			throws Exception
	{
		String canonicalName = "fr.doan.achilles.entity.metadata.ItIsAVeryLongClassNameNearing48Characters";

		String normalized = ColumnFamilyHelper.normalizeCanonicalName(canonicalName);

		assertThat(normalized).isEqualTo("ItIsAVeryLongClassNameNearing48Characters");
	}

	private void prepareData()
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		SimpleMeta<String> simplePropertyMeta = (SimpleMeta<String>) PropertyMetaFactory
				.factory(Void.class, String.class).type(SIMPLE).propertyName("name")
				.accessors(accessors).build();

		propertyMetas.put("name", simplePropertyMeta);

		meta = entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName("fr.doan.TestBean")
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
