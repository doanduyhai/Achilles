package info.archinnov.achilles.table;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.table.ThriftColumnFamilyFactory.COUNTER_KEY_ALIAS;
import static info.archinnov.achilles.table.ThriftColumnFamilyValidator.*;
import static me.prettyprint.hector.api.ddl.ComparatorType.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.Date;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftColumnFamilyValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftColumnFamilyValidator validator;

    @Mock
    private ThriftComparatorTypeAliasFactory comparatorAliasFactory;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private PropertyMeta<Long, String> propertyMeta;

    @Mock
    private ColumnFamilyDefinition cfDef;

    @Test
    public void should_exception_when_wrong_key_class_on_counter_column_family() throws Exception {

        when(cfDef.getKeyValidationClass()).thenReturn(ASCIITYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn("(alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'achillesCounterCF' key class 'org.apache.cassandra.db.marshal.AsciiType(alias)' should be '"
                        + COUNTER_KEY_CHECK + "'");

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_exception_when_wrong_key_type_alias_on_counter_column_family() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn("(wrong_alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'achillesCounterCF' key class 'org.apache.cassandra.db.marshal.CompositeType(wrong_alias)' should be '"
                        + COUNTER_KEY_CHECK + "'");

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_exception_when_wrong_comparator_type_on_counter_column_family() throws Exception {

        when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
        when(cfDef.getComparatorType()).thenReturn(ASCIITYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'achillesCounterCF' comparator type 'AsciiType(alias)' should be '"
                        + COUNTER_COMPARATOR_CHECK + "'");

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_exception_when_wrong_comparator_type_alias_on_counter_column_family() throws Exception {

        when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(wrong_alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'achillesCounterCF' comparator type 'CompositeType(wrong_alias)' should be '"
                        + COUNTER_COMPARATOR_CHECK + "'");

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_exception_when_wrong_validation_class_on_counter_column_family() throws Exception {

        when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(org.apache.cassandra.db.marshal.UTF8Type)");
        when(cfDef.getDefaultValidationClass()).thenReturn(ASCIITYPE.getClassName());

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'achillesCounterCF' validation class 'org.apache.cassandra.db.marshal.AsciiType' should be '"
                        + COUNTERTYPE.getClassName() + "'");

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_validate() throws Exception {

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(cfDef.getComparatorType()).thenReturn(ComparatorType.COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias())
                .thenReturn(
                        "(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)");
        validator.validateCFForEntity(cfDef, entityMeta);
    }

    @Test
    public void should_validate_counter_cf() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(COMPOSITETYPE.getClassName());
        when(cfDef.getKeyValidationAlias()).thenReturn(COUNTER_KEY_ALIAS);
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(org.apache.cassandra.db.marshal.UTF8Type)");
        when(cfDef.getDefaultValidationClass()).thenReturn(COUNTERTYPE.getClassName());

        validator.validateCounterCF(cfDef);
    }

    @Test
    public void should_exception_when_not_matching_key_validation_class() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(entityMeta.getTableName()).thenReturn("cf");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'cf' key class 'org.apache.cassandra.db.marshal.BytesType' does not correspond to the entity id class 'org.apache.cassandra.db.marshal.LongType'");

        validator.validateCFForEntity(cfDef, entityMeta);
    }

    @Test
    public void should_exception_when_comparator_type_null() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(entityMeta.getTableName()).thenReturn("cf");
        when(cfDef.getComparatorType()).thenReturn(null);

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The column family 'cf' comparator type 'null' should be '"
                + ENTITY_COMPARATOR_TYPE_CHECK + "'");

        validator.validateCFForEntity(cfDef, entityMeta);
    }

    @Test
    public void should_exception_when_comparator_type_not_composite() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(entityMeta.getTableName()).thenReturn("cf");
        when(cfDef.getComparatorType()).thenReturn(ComparatorType.ASCIITYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The column family 'cf' comparator type 'AsciiType(alias)' should be '"
                + ENTITY_COMPARATOR_TYPE_CHECK + "'");

        validator.validateCFForEntity(cfDef, entityMeta);
    }

    @Test
    public void should_exception_when_comparator_type_alias_does_not_match() throws Exception {
        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(entityMeta.getTableName()).thenReturn("cf");
        when(cfDef.getComparatorType()).thenReturn(ComparatorType.COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn("(wrong_alias)");

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The column family 'cf' comparator type 'CompositeType(wrong_alias)' should be '"
                + ENTITY_COMPARATOR_TYPE_CHECK + "'");

        validator.validateCFForEntity(cfDef, entityMeta);
    }

    @Test
    public void should_validate_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Date.class).type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setClusteredEntity(true);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn(
                "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
        when(cfDef.getDefaultValidationClass()).thenReturn(DATE_SRZ.getComparatorType().getClassName());

        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }

    @Test
    public void should_validate_join_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> joinIdMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(UserBean.class).type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn(
                "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
        when(cfDef.getDefaultValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());

        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }

    @Test
    public void should_validate_counter_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn(
                "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
        when(cfDef.getDefaultValidationClass()).thenReturn(ComparatorType.COUNTERTYPE.getClassName());

        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }

    @Test
    public void should_exception_when_wrong_key_validation_type_for_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Date.class).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The column family 'tableName' key validation type should be '"
                + LONG_SRZ.getComparatorType().getClassName() + "'");
        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }

    @Test
    public void should_exception_when_wrong_comparator_type_alias_for_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Date.class).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn(
                "(org.apache.cassandra.db.marshal.LongType,org.apache.cassandra.db.marshal.UUIDType)");

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'tableName' comparator type should be 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)'");
        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }

    @Test
    public void should_exception_when_wrong_validation_type_for_clustered_entity() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false)).thenReturn(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");

        when(cfDef.getKeyValidationClass()).thenReturn(LONG_SRZ.getComparatorType().getClassName());
        when(cfDef.getComparatorType()).thenReturn(COMPOSITETYPE);
        when(cfDef.getComparatorTypeAlias()).thenReturn(
                "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
        when(cfDef.getDefaultValidationClass()).thenReturn(INT_SRZ.getComparatorType().getClassName());

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The column family 'tableName' default validation type should be 'org.apache.cassandra.db.marshal.LongType'");
        validator.validateCFForClusteredEntity(cfDef, meta, "tableName");
    }
}
