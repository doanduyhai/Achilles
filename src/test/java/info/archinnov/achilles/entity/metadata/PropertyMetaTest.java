package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.UUID;

import mapping.entity.UserBean;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import testBuilders.PropertyMetaTestBuilder;

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

		KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(12, "12", 456);
		String keyValueString = objectMapper.writeValueAsString(keyValue);

		KeyValue<Integer, String> converted = propertyMeta.getKeyValueFromString(keyValueString);

		assertThat(converted.getKey()).isEqualTo(keyValue.getKey());
		assertThat(converted.getValue()).isEqualTo(keyValue.getValue());
		assertThat(converted.getTtl()).isEqualTo(keyValue.getTtl());
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
				.type(PropertyType.JOIN_WIDE_MAP)
				.build();

		Object bean = new UserBean();

		Object cast = propertyMeta.castValue(bean);

		assertThat(cast).isSameAs(bean);
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
	public void should_cast_value_as_map_value() throws Exception
	{
		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, UserBean.class)
				.type(PropertyType.MAP)
				.build();

		Object userBean = new UserBean();

		Object cast = propertyMeta.castValue(userBean);

		assertThat(cast).isSameAs(userBean);
	}

	@Test
	public void should_cast_value_as_lazy_map_value() throws Exception
	{
		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, UserBean.class)
				.type(PropertyType.LAZY_MAP)
				.build();

		Object userBean = new UserBean();

		Object cast = propertyMeta.castValue(userBean);

		assertThat(cast).isSameAs(userBean);
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

	@SuppressWarnings("unchecked")
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
	public void should_return_true_for_isJoinColumn() throws Exception
	{
		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
				.noClass(Void.class, String.class)
				.type(PropertyType.JOIN_SIMPLE)
				.build();

		assertThat(propertyMeta.isJoinColumn()).isTrue();
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
}
