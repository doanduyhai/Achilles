package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static fr.doan.achilles.metadata.builder.SimplePropertyMetaBuilder.simplePropertyMetaBuilder;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.Assertions.assertThat;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Charsets;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;

@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyBuilderTest {
    private final ColumnFamilyBuilder builder = new ColumnFamilyBuilder();

    private final Method[] accessors = new Method[2];

    private PropertyMeta<Long> idMeta;

    @Mock
    private Keyspace keyspace;

    @Before
    public void setUp() throws Exception {
        accessors[0] = TestBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
        accessors[1] = TestBean.class.getDeclaredMethod("setId", Long.class);

        idMeta = simplePropertyMetaBuilder(Long.class).propertyName("id").accessors(accessors).build();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_build_cf() throws Exception {
        class TestClass implements Serializable {
            private static final long serialVersionUID = 1L;
        }

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        propertyMetas.put("name", simplePropertyMetaBuilder(String.class).propertyName("name").accessors(accessors)
                .build());
        propertyMetas.put("age", simplePropertyMetaBuilder(Integer.class).propertyName("age").accessors(accessors)
                .build());
        propertyMetas.put("test", simplePropertyMetaBuilder(TestClass.class).propertyName("test")
                .accessors(accessors).build());

        EntityMeta<Long> entityMeta = entityMetaBuilder(idMeta).canonicalClassName("test.com.Entity")
                .columnFamilyName("myEntity").serialVersionUID(1L).keyspace(keyspace).propertyMetas(propertyMetas)
                .build();

        ColumnFamilyDefinition built = builder.build(entityMeta, "keyspace");

        assertThat(built).isNotNull();
        assertThat(built.getKeyspaceName()).isEqualTo("keyspace");
        assertThat(built.getKeyValidationClass()).isEqualTo("org.apache.cassandra.db.marshal.LongType");
        assertThat(built.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);

        assertThat(built.getColumnMetadata()).hasSize(3);
        assertThat(new String(built.getColumnMetadata().get(0).getName().array(), Charsets.UTF_8)).isEqualTo("test");
        assertThat(built.getColumnMetadata().get(0).getValidationClass()).isEqualTo(
                OBJECT_SRZ.getClass().getCanonicalName());

        assertThat(new String(built.getColumnMetadata().get(1).getName().array(), Charsets.UTF_8)).isEqualTo("age");
        assertThat(built.getColumnMetadata().get(1).getValidationClass()).isEqualTo(
                INT_SRZ.getClass().getCanonicalName());

        assertThat(new String(built.getColumnMetadata().get(2).getName().array(), Charsets.UTF_8)).isEqualTo("name");
        assertThat(built.getColumnMetadata().get(2).getValidationClass()).isEqualTo(
                STRING_SRZ.getClass().getCanonicalName());
    }

    class TestBean {

        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

    }
}
