package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.COUNTER_KEY_ALIAS;
import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.DYNAMIC_TYPE_ALIASES;
import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.UUID_SRZ;
import static me.prettyprint.hector.api.ddl.ComparatorType.ASCIITYPE;
import static me.prettyprint.hector.api.ddl.ComparatorType.COMPOSITETYPE;
import static me.prettyprint.hector.api.ddl.ComparatorType.COUNTERTYPE;
import static me.prettyprint.hector.api.ddl.ComparatorType.DYNAMICCOMPOSITETYPE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.InvalidColumnFamilyException;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.ImmutableMap;

/**
 * ColumnFamilyBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyHelperTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ColumnFamilyHelper columnFamilyHelper;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Long, String> propertyMeta;

	@Mock
	private Keyspace keyspace;

	@Mock
	private PropertyHelper helper;

	@Mock
	private ColumnFamilyDefinition cfDef;

	@Test
	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public void should_build_dynamic_composite_column_family() throws Exception
	{
		PropertyMeta<?, Long> propertyMeta = mock(PropertyMeta.class);
		when((Serializer) propertyMeta.getValueSerializer()).thenReturn(STRING_SRZ);

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("age", propertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("myCF");
		when(entityMeta.getClassName()).thenReturn("fr.doan.test.bean");

		ColumnFamilyDefinition cfDef = columnFamilyHelper.buildDynamicCompositeCF(entityMeta,
				"keyspace");

		assertThat(cfDef).isNotNull();
		assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(cfDef.getName()).isEqualTo("myCF");
		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.DYNAMICCOMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(
				LONG_SRZ.getComparatorType().getTypeName());
	}

	@Test
	public void should_build_composite_column_family() throws Exception
	{
		PropertyMeta<Integer, String> wideMapMeta = new PropertyMeta<Integer, String>();
		wideMapMeta.setValueClass(String.class);

		when(helper.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
				"typeAlias");

		ColumnFamilyDefinition cfDef = columnFamilyHelper.buildCompositeCF("keyspace", wideMapMeta,
				Long.class, "cf", "entity");

		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(
				LONG_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(
				STRING_SRZ.getComparatorType().getTypeName());

		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("typeAlias");

	}

	@Test
	public void should_build_composite_column_family_with_object_type() throws Exception
	{
		PropertyMeta<Integer, CompleteBean> wideMapMeta = new PropertyMeta<Integer, CompleteBean>();
		wideMapMeta.setValueClass(CompleteBean.class);
		wideMapMeta.setType(PropertyType.SIMPLE);

		when(helper.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
				"typeAlias");

		ColumnFamilyDefinition cfDef = columnFamilyHelper.buildCompositeCF("keyspace", wideMapMeta,
				Long.class, "cf", "entity");

		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(
				STRING_SRZ.getComparatorType().getTypeName());

	}

	@Test
	public void should_build_composite_column_family_with_join_object_type() throws Exception
	{
		PropertyMeta<Integer, CompleteBean> wideMapMeta = new PropertyMeta<Integer, CompleteBean>();
		wideMapMeta.setValueClass(CompleteBean.class);
		wideMapMeta.setType(PropertyType.JOIN_SIMPLE);

		PropertyMeta<Void, UUID> joinIdMeta = PropertyMetaTestBuilder.valueClass(UUID.class)
				.build();
		EntityMeta<UUID> joinMeta = new EntityMeta<UUID>();
		joinMeta.setIdMeta(joinIdMeta);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);
		wideMapMeta.setJoinProperties(joinProperties);

		when(helper.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
				"typeAlias");

		ColumnFamilyDefinition cfDef = columnFamilyHelper.buildCompositeCF("keyspace", wideMapMeta,
				Long.class, "cf", "entity");

		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(
				UUID_SRZ.getComparatorType().getTypeName());

	}

	@Test
	public void should_build_counter_column_family() throws Exception
	{

		ColumnFamilyDefinition cfDef = columnFamilyHelper.buildCounterCF("keyspace");

		assertThat(cfDef.getKeyValidationClass()).isEqualTo(COMPOSITETYPE.getTypeName());
		assertThat(cfDef.getKeyValidationAlias()).isEqualTo(COUNTER_KEY_ALIAS);
		assertThat(cfDef.getComparatorType()).isEqualTo(DYNAMICCOMPOSITETYPE);
		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo(DYNAMIC_TYPE_ALIASES);
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(COUNTERTYPE.getClassName());

	}

	@Test
	public void should_normalize_canonical_classname() throws Exception
	{
		String canonicalName = "org.achilles.entity.ClassName";

		String normalized = ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}

	@Test
	public void should_exception_when_wrong_key_class_on_counter_column_family() throws Exception
	{

		when(cfDef.getKeyValidationClass()).thenReturn(ASCIITYPE.getClassName());

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'achillesCounterCF' key class 'org.apache.cassandra.db.marshal.AsciiType' should be 'org.apache.cassandra.db.marshal.CompositeType'");

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@Test
	public void should_exception_when_wrong_key_type_alias_on_counter_column_family()
			throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
		when(cfDef.getKeyValidationAlias()).thenReturn("wrong_alias");

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'achillesCounterCF' key type alias 'wrong_alias' should be '"
						+ COUNTER_KEY_ALIAS + "'");

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@Test
	public void should_exception_when_wrong_comparator_type_on_counter_column_family()
			throws Exception
	{

		when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
		when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
		when(cfDef.getComparatorType()).thenReturn(ASCIITYPE);

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'achillesCounterCF' comparator type 'AsciiType' should be 'DynamicCompositeType'");

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@Test
	public void should_exception_when_wrong_comparator_type_alias_on_counter_column_family()
			throws Exception
	{

		when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
		when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
		when(cfDef.getComparatorType()).thenReturn(DYNAMICCOMPOSITETYPE);
		when(cfDef.getComparatorTypeAlias()).thenReturn("wrong_alias");

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'achillesCounterCF' comparator type alias 'wrong_alias' should be '"
						+ DYNAMIC_TYPE_ALIASES + "'");

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@Test
	public void should_exception_when_wrong_validation_class_on_counter_column_family()
			throws Exception
	{

		when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
		when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
		when(cfDef.getComparatorType()).thenReturn(DYNAMICCOMPOSITETYPE);
		when(cfDef.getComparatorTypeAlias()).thenReturn(DYNAMIC_TYPE_ALIASES);
		when(cfDef.getDefaultValidationClass()).thenReturn(ASCIITYPE.getClassName());

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'achillesCounterCF' validation class 'org.apache.cassandra.db.marshal.AsciiType' should be '"
						+ COUNTERTYPE.getClassName() + "'");

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@Test
	public void should_exception_when_even_class_name_exceeeds_48_characters() throws Exception
	{
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family name 'ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(canonicalName);

	}

	@Test
	public void should_validate() throws Exception
	{

		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		Whitebox.setInternalState(columnFamilyHelper, "COMPARATOR_TYPE_AND_ALIAS",
				ComparatorType.ASCIITYPE.getTypeName());
		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_validate_counter_cf() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
		when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
		when(cfDef.getComparatorType()).thenReturn(DYNAMICCOMPOSITETYPE);
		when(cfDef.getComparatorTypeAlias()).thenReturn(DYNAMIC_TYPE_ALIASES);
		when(cfDef.getDefaultValidationClass()).thenReturn(COUNTERTYPE.getClassName());

		columnFamilyHelper.validateCounterCF(cfDef);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_validate_column_family_direct_mapping() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);

		Map<String, PropertyMeta<Long, String>> propertyMetaMap = ImmutableMap.of("any",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.COUNTERTYPE);

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_exception_when_not_matching_key_validation_class() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'cf' key class 'org.apache.cassandra.db.marshal.BytesType' does not correspond to the entity id class 'org.apache.cassandra.db.marshal.LongType'");

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_exception_when_comparator_type_null() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when(cfDef.getComparatorType()).thenReturn(null);

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'cf' comparator type should be 'DynamicCompositeType(f=>org.apache.cassandra.db.marshal.FloatType,d=>org.apache.cassandra.db.marshal.DateType,e=>org.apache.cassandra.db.marshal.DecimalType,b=>org.apache.cassandra.db.marshal.BytesType,c=>org.apache.cassandra.db.marshal.BooleanType,a=>org.apache.cassandra.db.marshal.AsciiType,l=>org.apache.cassandra.db.marshal.LongType,j=>org.apache.cassandra.db.marshal.Int32Type,i=>org.apache.cassandra.db.marshal.IntegerType,u=>org.apache.cassandra.db.marshal.UUIDType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,z=>org.apache.cassandra.db.marshal.DoubleType,x=>org.apache.cassandra.db.marshal.LexicalUUIDType)'");

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_exception_when_comparator_type_not_composite() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'cf' comparator type should be 'DynamicCompositeType(f=>org.apache.cassandra.db.marshal.FloatType,d=>org.apache.cassandra.db.marshal.DateType,e=>org.apache.cassandra.db.marshal.DecimalType,b=>org.apache.cassandra.db.marshal.BytesType,c=>org.apache.cassandra.db.marshal.BooleanType,a=>org.apache.cassandra.db.marshal.AsciiType,l=>org.apache.cassandra.db.marshal.LongType,j=>org.apache.cassandra.db.marshal.Int32Type,i=>org.apache.cassandra.db.marshal.IntegerType,u=>org.apache.cassandra.db.marshal.UUIDType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,z=>org.apache.cassandra.db.marshal.DoubleType,x=>org.apache.cassandra.db.marshal.LexicalUUIDType)'");

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_exception_when_composite_type_alias_when_cf_direct_mapping_not_match()
			throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
		Map<String, PropertyMeta<Long, String>> propertyMetaMap = ImmutableMap.of("any",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'cf' comparator type should be 'CounterColumnType'");

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_validate_cf_with_property_meta() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.COUNTERTYPE);

		columnFamilyHelper.validateCFWithPropertyMeta(cfDef, propertyMeta, "external_cf");
	}

	@Test
	public void should_exception_when_comparator_type_alias_does_not_match() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		exception.expect(InvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family 'cf' comparator type should be 'DynamicCompositeType(f=>org.apache.cassandra.db.marshal.FloatType,d=>org.apache.cassandra.db.marshal.DateType,e=>org.apache.cassandra.db.marshal.DecimalType,b=>org.apache.cassandra.db.marshal.BytesType,c=>org.apache.cassandra.db.marshal.BooleanType,a=>org.apache.cassandra.db.marshal.AsciiType,l=>org.apache.cassandra.db.marshal.LongType,j=>org.apache.cassandra.db.marshal.Int32Type,i=>org.apache.cassandra.db.marshal.IntegerType,u=>org.apache.cassandra.db.marshal.UUIDType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,z=>org.apache.cassandra.db.marshal.DoubleType,x=>org.apache.cassandra.db.marshal.LexicalUUIDType)'");

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}
}
