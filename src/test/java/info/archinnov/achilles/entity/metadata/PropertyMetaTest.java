package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.holder.KeyValue;

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

		Object test = "\"test\"";

		String value = propertyMeta.getValueFromString(test);
		assertThat(value).isEqualTo("test");
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
}
