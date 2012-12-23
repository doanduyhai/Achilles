package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Maps;

import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.metadata.WideMapMeta;

@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyBuilderTest
{
	private final ColumnFamilyBuilder builder = new ColumnFamilyBuilder();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private Keyspace keyspace;

	@Mock
	private PropertyHelper helper;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(builder, "helper", helper);
	}

	@Test
	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public void should_build_column_family() throws Exception
	{
		PropertyMeta<?, Long> propertyMeta = mock(PropertyMeta.class);
		when((Serializer) propertyMeta.getValueSerializer()).thenReturn(STRING_SRZ);

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("age", propertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when((Serializer) entityMeta.getIdSerializer()).thenReturn(LONG_SRZ);
		when(entityMeta.getColumnFamilyName()).thenReturn("myCF");
		when(entityMeta.getCanonicalClassName()).thenReturn("fr.doan.test.bean");

		ColumnFamilyDefinition cfDef = builder.buildForEntity(entityMeta, "keyspace");

		assertThat(cfDef).isNotNull();
		assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(cfDef.getName()).isEqualTo("myCF");
		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.DYNAMICCOMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(
				LONG_SRZ.getComparatorType().getTypeName());
	}

	@Test
	public void should_build_wide_row() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, String> wideMapMeta = new WideMapMeta<Integer, String>();
		wideMapMeta.setValueClass(String.class);

		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("map", wideMapMeta);
		entityMeta.setPropertyMetas(propertyMap);
		entityMeta.setColumnFamilyName("cf");

		PropertyMeta<Void, Long> idMeta = new SimpleMeta<Long>();
		idMeta.setValueClass(Long.class);
		entityMeta.setIdMeta(idMeta);

		when(helper.determineCompatatorTypeAliasForWideRow(entityMeta, true)).thenReturn(
				"typeAlias");

		ColumnFamilyDefinition cfDef = builder.buildForWideRow(entityMeta, "keyspace");

		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(
				LONG_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(
				STRING_SRZ.getComparatorType().getTypeName());

		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("typeAlias");

	}
}
