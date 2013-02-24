package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mapping.entity.UserBean;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

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
	public void should_exception_when_cannot_instantiate_list() throws Exception
	{
		PropertyMeta<Void, String> listMeta = new PropertyMeta<Void, String>();
		List<String> list = listMeta.newListInstance();

		assertThat(list).isInstanceOf(ArrayList.class);
	}

	@Test
	public void should_exception_when_cannot_instantiate_set() throws Exception
	{

		PropertyMeta<Void, String> setMeta = new PropertyMeta<Void, String>();
		Set<String> set = setMeta.newSetInstance();

		assertThat(set).isInstanceOf(HashSet.class);
	}

	@Test
	public void should_exception_when_cannot_instantiate_map() throws Exception
	{

		PropertyMeta<Integer, String> mapMeta = new PropertyMeta<Integer, String>();
		Map<?, String> map = mapMeta.newMapInstance();

		assertThat(map).isInstanceOf(HashMap.class);
	}

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

		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

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

		UUID timeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

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
				.noClass(Integer.class, UserBean.class) //
				.type(PropertyType.JOIN_WIDE_MAP) //
				.build();

		Object bean = new UserBean();

		Object cast = propertyMeta.castValue(bean);

		assertThat(cast).isSameAs(bean);
	}

	@Test
	public void should_cast_value_as_supported_type() throws Exception
	{
		PropertyMeta<Integer, UUID> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, UUID.class) //
				.type(PropertyType.WIDE_MAP) //
				.build();

		Object uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		Object cast = propertyMeta.castValue(uuid);

		assertThat(cast).isSameAs(uuid);
	}

	@Test
	public void should_cast_value_as_string() throws Exception
	{
		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, UserBean.class) //
				.type(PropertyType.WIDE_MAP) //
				.build();

		UserBean bean = new UserBean();
		bean.setName("name");
		bean.setUserId(12L);

		UserBean cast = propertyMeta.castValue(objectMapper.writeValueAsString(bean));

		assertThat(cast.getName()).isEqualTo(bean.getName());
		assertThat(cast.getUserId()).isEqualTo(bean.getUserId());
	}
}
