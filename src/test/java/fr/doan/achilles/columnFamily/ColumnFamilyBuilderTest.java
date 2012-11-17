package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.Assertions.assertThat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.junit.Test;

import com.google.common.base.Charsets;

import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;

public class ColumnFamilyBuilderTest
{
	private ColumnFamilyBuilder builder = new ColumnFamilyBuilder();

	@Test
	public void should_build_cf() throws Exception
	{
		class TestClass implements Serializable
		{
			private static final long serialVersionUID = 1L;
		}

		Map<String, PropertyMeta<?>> propertieMetas = new HashMap<String, PropertyMeta<?>>();
		propertieMetas.put("name", new SimplePropertyMeta<String>("name", String.class));
		propertieMetas.put("age", new SimplePropertyMeta<Integer>("age", Integer.class));
		propertieMetas.put("test", new SimplePropertyMeta<TestClass>("age", TestClass.class));

		EntityMeta<Long> entityMeta = new EntityMeta<Long>(Long.class, "test.com.Entity", "myEntity", 1L, propertieMetas);

		ColumnFamilyDefinition built = builder.build(entityMeta, "keyspace");

		assertThat(built).isNotNull();
		assertThat(built.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(built.getKeyValidationClass()).isEqualTo("org.apache.cassandra.db.marshal.LongType");
		assertThat(built.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);

		assertThat(built.getColumnMetadata()).hasSize(3);
		assertThat(new String(built.getColumnMetadata().get(0).getName().array(), Charsets.UTF_8)).isEqualTo("test");
		assertThat(built.getColumnMetadata().get(0).getValidationClass()).isEqualTo(OBJECT_SRZ.getClass().getCanonicalName());

		assertThat(new String(built.getColumnMetadata().get(1).getName().array(), Charsets.UTF_8)).isEqualTo("age");
		assertThat(built.getColumnMetadata().get(1).getValidationClass()).isEqualTo(INT_SRZ.getClass().getCanonicalName());

		assertThat(new String(built.getColumnMetadata().get(2).getName().array(), Charsets.UTF_8)).isEqualTo("name");
		assertThat(built.getColumnMetadata().get(2).getValidationClass()).isEqualTo(STRING_SRZ.getClass().getCanonicalName());
	}
}
