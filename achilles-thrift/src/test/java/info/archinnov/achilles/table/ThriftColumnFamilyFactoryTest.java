package info.archinnov.achilles.table;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.UUID_SRZ;
import static info.archinnov.achilles.table.ThriftColumnFamilyFactory.COUNTER_COMPARATOR_TYPE_ALIAS;
import static info.archinnov.achilles.table.ThriftColumnFamilyFactory.COUNTER_KEY_ALIAS;
import static me.prettyprint.hector.api.ddl.ComparatorType.COMPOSITETYPE;
import static me.prettyprint.hector.api.ddl.ComparatorType.COUNTERTYPE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftColumnFamilyFactoryTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftColumnFamilyFactory factory;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private PropertyMeta<Long, String> propertyMeta;

    @Mock
    private Keyspace keyspace;

    @Mock
    private ThriftComparatorTypeAliasFactory comparatorAliasFactory;

    @Mock
    private ColumnFamilyDefinition cfDef;

    @Test
    public void should_create_entity_column_family() throws Exception {
        PropertyMeta<?, String> propertyMeta = mock(PropertyMeta.class);
        when(propertyMeta.getValueClass()).thenReturn(String.class);

        Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
        propertyMetas.put("age", propertyMeta);

        when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
        when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
        when(entityMeta.getTableName()).thenReturn("myCF");
        when(entityMeta.getClassName()).thenReturn("fr.doan.test.bean");

        ColumnFamilyDefinition cfDef = factory.createEntityCF(entityMeta, "keyspace");

        assertThat(cfDef).isNotNull();
        assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
        assertThat(cfDef.getName()).isEqualTo("myCF");
        assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
        assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
    }

    @Test
    public void should_create_composite_column_family() throws Exception {
        PropertyMeta<Integer, String> wideMapMeta = new PropertyMeta<Integer, String>();
        wideMapMeta.setValueClass(String.class);
        wideMapMeta.setType(PropertyType.WIDE_MAP);

        when(comparatorAliasFactory.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
                "typeAlias");

        ColumnFamilyDefinition cfDef = factory.createWideRowCF("keyspace", wideMapMeta, Long.class, "cf", "entity");

        assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
        assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(STRING_SRZ.getComparatorType().getTypeName());

        assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("typeAlias");

    }

    @Test
    public void should_create_composite_column_family_with_object_type() throws Exception {
        PropertyMeta<Integer, CompleteBean> wideMapMeta = new PropertyMeta<Integer, CompleteBean>();
        wideMapMeta.setValueClass(CompleteBean.class);
        wideMapMeta.setType(PropertyType.SIMPLE);

        when(comparatorAliasFactory.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
                "typeAlias");

        ColumnFamilyDefinition cfDef = factory.createWideRowCF("keyspace", wideMapMeta, Long.class, "cf", "entity");

        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(STRING_SRZ.getComparatorType().getTypeName());

    }

    @Test
    public void should_create_composite_column_family_with_join_object_type() throws Exception {
        PropertyMeta<Integer, CompleteBean> wideMapMeta = new PropertyMeta<Integer, CompleteBean>();
        wideMapMeta.setValueClass(CompleteBean.class);
        wideMapMeta.setType(PropertyType.JOIN_SIMPLE);

        PropertyMeta<Void, UUID> joinIdMeta = PropertyMetaTestBuilder.valueClass(UUID.class).build();
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);
        JoinProperties joinProperties = new JoinProperties();
        joinProperties.setEntityMeta(joinMeta);
        wideMapMeta.setJoinProperties(joinProperties);

        when(comparatorAliasFactory.determineCompatatorTypeAliasForCompositeCF(wideMapMeta, true)).thenReturn(
                "typeAlias");

        ColumnFamilyDefinition cfDef = factory.createWideRowCF("keyspace", wideMapMeta, Long.class, "cf", "entity");

        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(UUID_SRZ.getComparatorType().getTypeName());

    }

    @Test
    public void should_create_clustered_entity_column_family() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.completeBean(Void.class, Integer.class).field("name")
                .type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("tableName");
        meta.setClassName("entityName");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, true)).thenReturn(
                "(UTF8Type,UUIDType)");
        ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

        assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
        assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITE_SRZ.getComparatorType());
        assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("(UTF8Type,UUIDType)");
        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(INT_SRZ.getComparatorType().getTypeName());
    }

    @Test
    public void should_create_clustered_entity_column_family_with_counter_clustered_value() throws Exception {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("name")
                .type(PropertyType.COUNTER).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, true)).thenReturn(
                "(UTF8Type,UUIDType)");
        ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(COUNTERTYPE.getTypeName());
    }

    @Test
    public void should_create_clustered_entity_column_family_with_join_clustered_value() throws Exception {

        PropertyMeta<?, ?> joinIdMeta = PropertyMetaTestBuilder.valueClass(UUID.class).build();
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("name")
                .type(PropertyType.JOIN_SIMPLE).joinMeta(joinMeta).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", pm));

        when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta, true)).thenReturn(
                "(UTF8Type,UUIDType)");
        ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(UUID_SRZ.getComparatorType().getTypeName());
    }

    @Test
    public void should_create_counter_column_family() throws Exception {

        ColumnFamilyDefinition cfDef = factory.createCounterCF("keyspace");

        assertThat(cfDef.getKeyValidationClass()).isEqualTo(COMPOSITETYPE.getTypeName());
        assertThat(cfDef.getKeyValidationAlias()).isEqualTo(COUNTER_KEY_ALIAS);
        assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITETYPE);
        assertThat(cfDef.getComparatorTypeAlias()).isEqualTo(COUNTER_COMPARATOR_TYPE_ALIAS);
        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(COUNTERTYPE.getClassName());

    }

    @Test
    public void should_create_counter_wide_row() throws Exception {
        PropertyMeta<Integer, Long> counterWideMapMeta = new PropertyMeta<Integer, Long>();
        counterWideMapMeta.setValueClass(Long.class);
        counterWideMapMeta.setType(PropertyType.COUNTER_WIDE_MAP);

        when(comparatorAliasFactory.determineCompatatorTypeAliasForCompositeCF(counterWideMapMeta, true)).thenReturn(
                "typeAlias");

        ColumnFamilyDefinition cfDef = factory.createWideRowCF("keyspace", counterWideMapMeta, Long.class, "cf",
                "entity");

        assertThat(cfDef.getDefaultValidationClass()).isEqualTo(ComparatorType.COUNTERTYPE.getTypeName());
    }

}
