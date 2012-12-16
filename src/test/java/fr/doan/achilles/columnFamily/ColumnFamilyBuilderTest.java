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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;

@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyBuilderTest
{
	private final ColumnFamilyBuilder builder = new ColumnFamilyBuilder();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private Keyspace keyspace;

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

		ColumnFamilyDefinition cfDef = builder.build(entityMeta, "keyspace");

		assertThat(cfDef).isNotNull();
		assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(cfDef.getName()).isEqualTo("myCF");
		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.DYNAMICCOMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(
				LONG_SRZ.getComparatorType().getTypeName());
	}
}
