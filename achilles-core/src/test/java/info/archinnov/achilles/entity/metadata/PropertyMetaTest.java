package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;
import info.archinnov.achilles.type.KeyValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import com.google.common.collect.Sets;

/**
 * PropertyMetaTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaTest
{

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void should_get_value_from_string_with_correct_type() throws Exception
    {
        PropertyMeta<Void, Long> meta = new PropertyMeta<Void, Long>();
        meta.setValueClass(Long.class);
        meta.setObjectMapper(objectMapper);

        Object testString = "123";

        Object casted = meta.getValueFromString(testString);

        assertThat(casted).isInstanceOf(Long.class);
    }

    @Test
    public void should_get_value_from_string() throws Exception
    {
        PropertyMeta<String, String> propertyMeta = new PropertyMeta<String, String>();
        propertyMeta.setValueClass(String.class);
        propertyMeta.setObjectMapper(objectMapper);

        Object test = "test";

        String value = propertyMeta.getValueFromString(test);
        assertThat(value).isEqualTo("test");
    }

    @Test
    public void should_get_primitive_value_from_string() throws Exception
    {

        PropertyMeta<String, Boolean> propertyMeta = new PropertyMeta<String, Boolean>();
        propertyMeta.setValueClass(boolean.class);
        propertyMeta.setObjectMapper(objectMapper);

        Object test = "true";

        boolean value = propertyMeta.getValueFromString(test);
        assertThat(value).isTrue();
    }

    @Test
    public void should_get_enum_value_from_string() throws Exception
    {

        PropertyMeta<String, PropertyType> propertyMeta = new PropertyMeta<String, PropertyType>();
        propertyMeta.setValueClass(PropertyType.class);
        propertyMeta.setObjectMapper(objectMapper);

        Object test = "\"JOIN_MAP\"";

        PropertyType value = propertyMeta.getValueFromString(test);
        assertThat(value).isEqualTo(PropertyType.JOIN_MAP);
    }

    @Test
    public void should_get_allowed_type_from_string() throws Exception
    {

        PropertyMeta<String, UUID> propertyMeta = new PropertyMeta<String, UUID>();
        propertyMeta.setValueClass(UUID.class);
        propertyMeta.setObjectMapper(objectMapper);

        UUID uuid = new UUID(10L, 100L);

        Object test = objectMapper.writeValueAsString(uuid);

        UUID value = propertyMeta.getValueFromString(test);
        assertThat(value).isEqualTo(uuid);
    }

    @Test
    public void should_get_key_value_from_string() throws Exception
    {
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setObjectMapper(objectMapper);

        KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(12, "12", 456, 10L);
        String keyValueString = objectMapper.writeValueAsString(keyValue);

        KeyValue<Integer, String> converted = propertyMeta.getKeyValueFromString(keyValueString);

        assertThat(converted.getKey()).isEqualTo(keyValue.getKey());
        assertThat(converted.getValue()).isEqualTo(keyValue.getValue());
        assertThat(converted.getTtl()).isEqualTo(keyValue.getTtl());
        assertThat(converted.getTimestamp()).isEqualTo(10L);
    }

    @Test
    public void should_write_value_to_string() throws Exception
    {
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setObjectMapper(objectMapper);

        UUID timeUUID = new UUID(10L, 100L);

        String uuidString = objectMapper.writeValueAsString(timeUUID);

        String converted = propertyMeta.writeValueToString(timeUUID);

        assertThat(converted).isEqualTo(uuidString);
    }

    @Test
    public void should_write_value_as_supported_type() throws Exception
    {
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setObjectMapper(objectMapper);
        propertyMeta.setValueClass(String.class);

        String value = "test";

        Object converted = propertyMeta.writeValueAsSupportedTypeOrString(value);

        assertThat(converted).isSameAs(value);
    }

    @Test
    public void should_write_value_as_string_because_not_supported_type() throws Exception
    {
        PropertyMeta<Integer, UserBean> propertyMeta = new PropertyMeta<Integer, UserBean>();
        propertyMeta.setObjectMapper(objectMapper);

        UserBean value = new UserBean();
        String stringValue = objectMapper.writeValueAsString(value);

        Object converted = propertyMeta.writeValueAsSupportedTypeOrString(value);

        assertThat(converted).isEqualTo(stringValue);
    }

    @Test
    public void should_cast_key() throws Exception
    {
        PropertyMeta<String, String> propertyMeta = new PropertyMeta<String, String>();
        propertyMeta.setKeyClass(String.class);
        propertyMeta.setObjectMapper(objectMapper);

        Object test = "test";

        String key = propertyMeta.getKey(test);
        assertThat(key).isEqualTo("test");
    }

    @Test
    public void should_cast_value_as_join_type() throws Exception
    {
        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.JOIN_SIMPLE)
                .build();

        UserBean uuid = new UserBean();

        Object cast = propertyMeta.castValue(uuid);

        assertThat(cast).isSameAs(uuid);
    }

    @Test
    public void should_cast_value_as_supported_type() throws Exception
    {
        PropertyMeta<Integer, UUID> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UUID.class)
                .type(PropertyType.WIDE_MAP)
                .build();

        Object uuid = new UUID(10L, 100L);

        Object cast = propertyMeta.castValue(uuid);

        assertThat(cast).isSameAs(uuid);
    }

    @Test
    public void should_cast_value_as_string() throws Exception
    {
        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.WIDE_MAP)
                .build();

        UserBean bean = new UserBean();
        bean.setName("name");
        bean.setUserId(12L);

        UserBean cast = propertyMeta.castValue(objectMapper.writeValueAsString(bean));

        assertThat(cast.getName()).isEqualTo(bean.getName());
        assertThat(cast.getUserId()).isEqualTo(bean.getUserId());
    }

    @Test
    public void should_return_join_entity_meta() throws Exception
    {
        EntityMeta joinMeta = new EntityMeta();

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.JOIN_WIDE_MAP)
                .joinMeta(joinMeta)
                .build();

        assertThat(propertyMeta.joinMeta()).isSameAs(joinMeta);
    }

    @Test
    public void should_return_null_if_not_join_type() throws Exception
    {
        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.WIDE_MAP)
                .build();

        assertThat(propertyMeta.joinMeta()).isNull();
    }

    @Test
    public void should_return_join_id_meta() throws Exception
    {
        EntityMeta joinMeta = new EntityMeta();
        PropertyMeta<Void, Long> joinIdMeta = new PropertyMeta<Void, Long>();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.JOIN_WIDE_MAP)
                .joinMeta(joinMeta)
                .build();

        assertThat((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).isSameAs(joinIdMeta);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_return_counter_id_meta() throws Exception
    {
        PropertyMeta<Void, String> idMeta = new PropertyMeta<Void, String>();

        PropertyMeta<Void, Long> propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class) //
                .type(PropertyType.COUNTER)
                //
                .counterIdMeta(idMeta)
                //
                .build();

        assertThat((PropertyMeta<Void, String>) propertyMeta.counterIdMeta()).isSameAs(idMeta);
    }

    public void should_return_fqcn() throws Exception
    {

        PropertyMeta<Void, Long> propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class) //
                .type(PropertyType.COUNTER)
                .fqcn("fqcn")
                .build();

        assertThat(propertyMeta.fqcn()).isEqualTo("fqcn");
    }

    @Test
    public void should_return_null_when_joinid_invoke() throws Exception
    {
        EntityMeta joinMeta = new EntityMeta();

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.JOIN_WIDE_MAP)
                .joinMeta(joinMeta)
                .build();

        assertThat(propertyMeta.joinIdMeta()).isNull();
    }

    @Test
    public void should_return_true_when_join_type() throws Exception
    {
        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .type(PropertyType.JOIN_SIMPLE)
                .build();

        assertThat(propertyMeta.isJoin()).isTrue();
    }

    @Test
    public void should_return_true_for_isLazy() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Void.class, String.class)
                .type(PropertyType.LAZY_LIST)
                .build();

        assertThat(propertyMeta.isLazy()).isTrue();
    }

    @Test
    public void should_return_true_for_isWideMap() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Void.class, String.class)
                //
                .type(PropertyType.WIDE_MAP)
                //
                .build();

        assertThat(propertyMeta.isWideMap()).isTrue();
    }

    @Test
    public void should_return_true_for_isCounter_when_type_is_counter() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Void.class, String.class)
                .type(PropertyType.COUNTER)
                .build();

        assertThat(propertyMeta.isCounter()).isTrue();
    }

    @Test
    public void should_return_true_for_isCounter_when_type_is_counter_external_widemap()
            throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Void.class, String.class)
                .type(PropertyType.COUNTER_WIDE_MAP)
                .build();

        assertThat(propertyMeta.isCounter()).isTrue();
    }

    @Test
    public void should_have_cascade_type() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .cascadeType(MERGE)
                .build();

        assertThat(pm.hasCascadeType(MERGE)).isTrue();
    }

    @Test
    public void should_not_have_cascade_type() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .cascadeType(MERGE)
                .build();

        assertThat(pm.hasCascadeType(ALL)).isFalse();
    }

    @Test
    public void should_not_have_cascade_type_because_not_join() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .build();

        assertThat(pm.hasCascadeType(ALL)).isFalse();
    }

    @Test
    public void should_have_any_cascade_type() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
                .cascadeTypes(MERGE, ALL).build();

        assertThat(pm.hasAnyCascadeType(MERGE, PERSIST, REMOVE)).isTrue();
        assertThat(pm.getJoinProperties().getCascadeTypes()).containsOnly(MERGE, ALL);
        assertThat(pm.hasAnyCascadeType(PERSIST, REMOVE)).isFalse();

    }

    @Test
    public void should_return_true_if_type_is_join_collection() throws Exception
    {
        PropertyMeta<?, ?> pm1 = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(JOIN_LIST)
                .build();

        PropertyMeta<?, ?> pm2 = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(JOIN_SET)
                .build();

        assertThat(pm1.isJoinCollection()).isTrue();
        assertThat(pm2.isJoinCollection()).isTrue();
    }

    @Test
    public void should_return_true_if_type_is_join_map() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(JOIN_MAP)
                .build();
        assertThat(pm.isJoinMap()).isTrue();
    }

    @Test
    public void should_use_equals_and_hashcode() throws Exception
    {
        PropertyMeta<Void, String> meta1 = new PropertyMeta<Void, String>();
        meta1.setEntityClassName("entity");
        meta1.setPropertyName("field1");
        meta1.setType(PropertyType.SIMPLE);

        PropertyMeta<Void, String> meta2 = new PropertyMeta<Void, String>();
        meta2.setEntityClassName("entity");
        meta2.setPropertyName("field2");
        meta2.setType(PropertyType.SIMPLE);

        PropertyMeta<Void, String> meta3 = new PropertyMeta<Void, String>();
        meta3.setEntityClassName("entity");
        meta3.setPropertyName("field1");
        meta3.setType(PropertyType.LIST);

        PropertyMeta<Void, String> meta4 = new PropertyMeta<Void, String>();
        meta4.setEntityClassName("entity1");
        meta4.setPropertyName("field1");
        meta4.setType(PropertyType.SIMPLE);

        PropertyMeta<Void, String> meta5 = new PropertyMeta<Void, String>();
        meta5.setEntityClassName("entity");
        meta5.setPropertyName("field1");
        meta5.setType(PropertyType.SIMPLE);

        assertThat(meta1).isNotEqualTo(meta2);
        assertThat(meta1).isNotEqualTo(meta3);
        assertThat(meta1).isNotEqualTo(meta4);
        assertThat(meta1).isEqualTo(meta5);

        assertThat(meta1.hashCode()).isNotEqualTo(meta2.hashCode());
        assertThat(meta1.hashCode()).isNotEqualTo(meta3.hashCode());
        assertThat(meta1.hashCode()).isNotEqualTo(meta4.hashCode());
        assertThat(meta1.hashCode()).isEqualTo(meta5.hashCode());

        Set<PropertyMeta<Void, String>> pms = Sets.newHashSet(meta1, meta2, meta3, meta4, meta5);

        assertThat(pms).containsOnly(meta1, meta2, meta3, meta4);
    }

    @Test
    public void should_get_cql_property_name() throws Exception
    {
        PropertyMeta<?, ?> pm = new PropertyMeta<Void, String>();
        pm.setPropertyName("Name");

        assertThat(pm.getCQLPropertyName()).isEqualTo("name");
    }

    @Test
    public void should_get_cql_external_table_name() throws Exception
    {
        PropertyMeta<?, ?> pm = new PropertyMeta<Void, String>();
        pm.setExternalTableName("taBLe");

        assertThat(pm.getCQLExternalTableName()).isEqualTo("table");
    }

    @Test
    public void should_serialize_as_json() throws Exception
    {
        PropertyMeta<Void, UUID> pm = new PropertyMeta<Void, UUID>();
        pm.setObjectMapper(objectMapper);

        assertThat(pm.jsonSerializeValue(new UUID(10, 10))).isEqualTo(
                "\"00000000-0000-000a-0000-00000000000a\"");
    }

    @Test
    public void should_get_cql_ordering_component() throws Exception
    {
        PropertyMeta<Void, String> meta = new PropertyMeta<Void, String>();
        CompoundKeyProperties multiKeyProperties = new CompoundKeyProperties();
        multiKeyProperties.setComponentNames(Arrays.asList("id", "age", "name"));
        meta.setCompoundKeyProperties(multiKeyProperties);

        assertThat(meta.getCQLOrderingComponent()).isEqualTo("age");
    }

    @Test
    public void should_return_null_for_cql_ordering_component_if_no_multikey() throws Exception
    {
        PropertyMeta<Void, String> meta = new PropertyMeta<Void, String>();

        assertThat(meta.getCQLOrderingComponent()).isNull();

    }

    @Test
    public void should_get_cql_component_names() throws Exception
    {
        PropertyMeta<Void, String> meta = new PropertyMeta<Void, String>();
        CompoundKeyProperties multiKeyProperties = new CompoundKeyProperties();
        multiKeyProperties.setComponentNames(Arrays.asList("Id", "aGe", "namE"));
        meta.setCompoundKeyProperties(multiKeyProperties);

        assertThat(meta.getCQLComponentNames()).containsExactly("id", "age", "name");
    }

    @Test
    public void should_return_empty_list_when_no_cql_component_names() throws Exception
    {
        PropertyMeta<Void, String> meta = new PropertyMeta<Void, String>();
        assertThat(meta.getCQLComponentNames()).isEmpty();
    }

    @Test
    public void should_return_compound_key_constructor() throws Exception
    {
        Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
                .getConstructor(Long.class,
                        String.class);
        CompoundKeyProperties props = new CompoundKeyProperties();
        props.setConstructor(constructor);

        PropertyMeta<?, ?> meta = new PropertyMeta<Void, CompoundKeyByConstructor>();
        meta.setCompoundKeyProperties(props);

        assertThat(meta.<CompoundKeyByConstructor> getCompoundKeyConstructor()).isSameAs(
                constructor);
    }

    @Test
    public void should_return_null_when_no_compound_key_constructor() throws Exception
    {
        PropertyMeta<?, ?> meta = new PropertyMeta<Void, CompoundKeyByConstructor>();
        assertThat(meta.getCompoundKeyConstructor()).isNull();
    }

    @Test
    public void should_return_true_when_compound_key_has_default_constructor() throws Exception
    {

        Constructor<TweetCompoundKey> constructor = TweetCompoundKey.class.getConstructor();
        CompoundKeyProperties props = new CompoundKeyProperties();
        props.setConstructor(constructor);

        PropertyMeta<?, ?> meta = new PropertyMeta<Void, CompoundKeyByConstructor>();
        meta.setCompoundKeyProperties(props);

        assertThat(meta.hasDefaultConstructorForCompoundKey()).isTrue();
    }

    @Test
    public void should_return_false_when_compound_key_has_no_default_constructor() throws Exception
    {

        Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
                .getConstructor(Long.class,
                        String.class);
        CompoundKeyProperties props = new CompoundKeyProperties();
        props.setConstructor(constructor);

        PropertyMeta<?, ?> meta = new PropertyMeta<Void, CompoundKeyByConstructor>();
        meta.setCompoundKeyProperties(props);

        assertThat(meta.hasDefaultConstructorForCompoundKey()).isFalse();
    }

    @Test
    public void should_return_false_when_no_compound_key_constructor() throws Exception
    {
        PropertyMeta<?, ?> meta = new PropertyMeta<Void, CompoundKeyByConstructor>();
        assertThat(meta.hasDefaultConstructorForCompoundKey()).isFalse();
    }

    @Test
    public void should_get_component_getters() throws Exception
    {
        Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compGetters(Arrays.asList(idGetter, nameGetter))
                .build();

        assertThat(idMeta.getComponentGetters()).containsExactly(idGetter, nameGetter);
    }

    @Test
    public void should_return_empty_list_when_no_component_getters() throws Exception
    {

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).build();

        assertThat(idMeta.getComponentGetters()).isEmpty();
    }

    @Test
    public void should_get_partition_key_getter() throws Exception
    {
        Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compGetters(Arrays.asList(idGetter, nameGetter))
                .build();

        assertThat(idMeta.getPartitionKeyGetter()).isEqualTo(idGetter);
    }

    @Test
    public void should_get_partition_key_setter() throws Exception
    {
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compSetters(Arrays.asList(idSetter, nameSetter))
                .build();

        assertThat(idMeta.getPartitionKeySetter()).isEqualTo(idSetter);
    }

    @Test
    public void should_return_null_when_no_partition_key_getter() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).build();

        assertThat(idMeta.getPartitionKeyGetter()).isNull();
    }

    @Test
    public void should_return_null_when_no_partition_key_setter() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).build();

        assertThat(idMeta.getPartitionKeySetter()).isNull();
    }

    @Test
    public void should_get_component_setters() throws Exception
    {
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compSetters(Arrays.asList(idSetter, nameSetter))
                .build();

        assertThat(idMeta.getComponentSetters()).containsExactly(idSetter, nameSetter);
    }

    @Test
    public void should_return_empty_list_when_no_component_setters() throws Exception
    {

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).build();

        assertThat(idMeta.getComponentSetters()).isEmpty();
    }

    @Test
    public void should_get_component_classes() throws Exception
    {

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compClasses(Arrays.<Class<?>> asList(Long.class, String.class))
                .build();

        assertThat(idMeta.getComponentClasses()).containsExactly(Long.class, String.class);
    }

    @Test
    public void should_return_empty_list_when_no_component_classes() throws Exception
    {

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).build();

        assertThat(idMeta.getComponentClasses()).isEmpty();
    }

    @Test
    public void should_return_true_for_is_embedded_id() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class)
                .type(EMBEDDED_ID)
                .build();

        assertThat(idMeta.isEmbeddedId()).isTrue();
    }

    @Test
    public void should_return_false_for_is_embedded_id() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder.valueClass(
                CompoundKey.class).type(ID).build();

        assertThat(idMeta.isEmbeddedId()).isFalse();
    }

    @Test
    public void should_get_value_from_cassandra_for_join_id() throws Exception
    {

        Long cassandraValue = 11L;
        PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .type(JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .mapper(objectMapper)
                .build();

        Object actual = pm.getValueFromCassandra(cassandraValue);

        assertThat(actual).isEqualTo(11L);
    }

    @Test
    public void should_get_value_from_cassandra_for_string() throws Exception
    {
        Object cassandraValue = "string";
        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Object actual = pm.getValueFromCassandra(cassandraValue);

        assertThat(actual).isEqualTo("string");
    }

    @Test
    public void should_get_value_from_cassandra_for_serialized_entity() throws Exception
    {
        UserBean bean = new UserBean();
        bean.setUserId(11L);
        bean.setName("name");

        String serialized = objectMapper.writeValueAsString(bean);

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .type(SIMPLE)
                .mapper(objectMapper)
                .build();

        Object actual = pm.getValueFromCassandra(serialized);

        assertThat(actual).isInstanceOf(UserBean.class);

        UserBean actualBean = (UserBean) actual;

        assertThat(actualBean.getUserId()).isEqualTo(11L);
        assertThat(actualBean.getName()).isEqualTo("name");
    }
}
