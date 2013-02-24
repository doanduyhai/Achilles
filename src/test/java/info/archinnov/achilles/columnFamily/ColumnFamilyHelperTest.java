package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.InvalidColumnFamilyException;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

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

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_not_matching_key_validation_class() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_null() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(null);

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_not_composite() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_composite_type_alias_when_cf_direct_mapping_not_match()
			throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
		Map<String, PropertyMeta<Long, String>> propertyMetaMap = ImmutableMap.of("any",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

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

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_alias_does_not_match() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when(entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		columnFamilyHelper.validateCFWithEntityMeta(cfDef, entityMeta);
	}
}
