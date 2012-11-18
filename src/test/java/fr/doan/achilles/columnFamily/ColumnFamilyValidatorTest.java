package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static fr.doan.achilles.metadata.builder.SimplePropertyMetaBuilder.simplePropertyMetaBuilder;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class ColumnFamilyValidatorTest {

    private final ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();

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
    public void should_validate_column_family() throws Exception {
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(null, "testCf",
                ComparatorType.COMPOSITETYPE);
        cfDef.setKeyValidationClass(Utils.LONG_SRZ.getComparatorType().getTypeName());

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        SimplePropertyMeta<String> simplePropertyMeta = simplePropertyMetaBuilder(String.class).propertyName("name")
                .accessors(accessors).build();

        propertyMetas.put("name", simplePropertyMeta);

        EntityMeta<Long> meta = entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName("test.MyClass")
                .serialVersionUID(1L).propertyMetas(propertyMetas).build();

        columnFamilyValidator.validate(cfDef, meta);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = InvalidColumnFamilyException.class)
    public void should_exception_not_matching_keyClass() throws Exception {
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(null, "testCf",
                ComparatorType.COMPOSITETYPE);
        cfDef.setKeyValidationClass(Utils.INT_SRZ.getComparatorType().getTypeName());

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        SimplePropertyMeta<String> simplePropertyMeta = simplePropertyMetaBuilder(String.class).propertyName("name")
                .accessors(accessors).build();

        propertyMetas.put("name", simplePropertyMeta);

        EntityMeta<Long> meta = entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName("test.MyClass")
                .serialVersionUID(1L).propertyMetas(propertyMetas).build();

        columnFamilyValidator.validate(cfDef, meta);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = InvalidColumnFamilyException.class)
    public void should_exception_not_matching_column_comparator() throws Exception {
        ColumnFamilyDefinition cfDef = HFactory
                .createColumnFamilyDefinition(null, "testCf", ComparatorType.BYTESTYPE);
        cfDef.setKeyValidationClass(Utils.LONG_SRZ.getComparatorType().getTypeName());

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();

        SimplePropertyMeta<String> simplePropertyMeta = simplePropertyMetaBuilder(String.class).propertyName("name")
                .accessors(accessors).build();

        propertyMetas.put("name", simplePropertyMeta);

        EntityMeta<Long> meta = entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName("test.MyClass")
                .serialVersionUID(1L).propertyMetas(propertyMetas).build();

        columnFamilyValidator.validate(cfDef, meta);
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
