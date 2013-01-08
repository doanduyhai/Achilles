package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * PropertyMetaTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaTest
{

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
	public void should_cast_value_to_correct_type() throws Exception
	{
		PropertyMeta<Void, String> meta = new PropertyMeta<Void, String>();
		meta.setValueClass(String.class);

		Object testString = "test";

		Object casted = meta.getValue(testString);

		assertThat(casted).isInstanceOf(String.class);
	}

	@Test
	public void should_cast_key() throws Exception
	{
		PropertyMeta<String, String> propertyMeta = new PropertyMeta<String, String>();
		propertyMeta.setKeyClass(String.class);

		Object test = "test";

		String key = propertyMeta.getKey(test);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_cast_value() throws Exception
	{
		PropertyMeta<String, String> propertyMeta = new PropertyMeta<String, String>();
		propertyMeta.setValueClass(String.class);

		Object test = "test";

		String value = propertyMeta.getValue(test);
		assertThat(value).isEqualTo("test");
	}
}
