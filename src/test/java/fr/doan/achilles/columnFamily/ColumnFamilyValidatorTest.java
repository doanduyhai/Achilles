package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.mockito.Mockito.when;

import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableMap;

import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.InvalidColumnFamilyException;

/**
 * ColumnFamilyValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyValidatorTest
{

	@InjectMocks
	private ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();

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

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(columnFamilyValidator, "helper", helper);
	}

	@Test
	public void should_validate() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		ReflectionTestUtils.setField(columnFamilyValidator, "COMPARATOR_TYPE_AND_ALIAS",
				ComparatorType.ASCIITYPE.getTypeName());
		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_widerow() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.isWideRow()).thenReturn(true);

		Map<String, PropertyMeta<Long, String>> propertyMetaMap = ImmutableMap.of("any",
				propertyMeta);
		when(entityMeta.getPropertyMetas()).thenReturn((Map) propertyMetaMap);

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.COUNTERTYPE);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_not_matching_key_validation_class() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_null() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(null);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_not_composite() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_composite_type_alias_for_widerow_not_match() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.isWideRow()).thenReturn(true);
		Map<String, PropertyMeta<Long, String>> propertyMetaMap = ImmutableMap.of("any",
				propertyMeta);
		when(entityMeta.getPropertyMetas()).thenReturn((Map) propertyMetaMap);

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}

	@Test
	public void should_validate_cf_with_property_meta() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());

		when(helper.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false)).thenReturn(
				ComparatorType.COUNTERTYPE.getTypeName());
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.COUNTERTYPE);

		columnFamilyValidator.validateCFWithPropertyMeta(cfDef, propertyMeta, "external_cf");
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_comparator_type_alias_does_not_match() throws Exception
	{
		when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);

		columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
	}
}
