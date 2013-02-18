package info.archinnov.achilles.columnFamily;

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

/**
 * ColumnFamilyBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyBuilderTest
{
	@InjectMocks
	private ColumnFamilyBuilder builder;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private Keyspace keyspace;

	@Mock
	private PropertyHelper helper;

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

		ColumnFamilyDefinition cfDef = builder.buildDynamicCompositeCF(entityMeta, "keyspace");

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

		ColumnFamilyDefinition cfDef = builder.buildCompositeCF("keyspace", wideMapMeta,
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

		String normalized = ColumnFamilyBuilder
				.normalizerAndValidateColumnFamilyName(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_when_even_class_name_exceeeds_48_characters() throws Exception
	{
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		ColumnFamilyBuilder.normalizerAndValidateColumnFamilyName(canonicalName);

	}
}
