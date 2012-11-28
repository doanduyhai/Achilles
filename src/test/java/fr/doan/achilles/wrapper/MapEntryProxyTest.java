package fr.doan.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.MapEntryProxy;

@RunWith(MockitoJUnitRunner.class)
public class MapEntryProxyTest
{
	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_mark_dirty_on_value_set() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");
		Entry<Integer, String> mapEntry = map.entrySet().iterator().next();

		MapEntryProxy<Integer, String> mapEntryproxy = new MapEntryProxy<Integer, String>(mapEntry);

		mapEntryproxy.setDirtyMap(dirtyMap);
		mapEntryproxy.setSetter(setter);
		mapEntryproxy.setPropertyMeta(propertyMeta);

		mapEntryproxy.setValue("TEST");

		verify(dirtyMap).put(setter, propertyMeta);

	}

	@Test
	public void should_equal() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.equals(proxy2)).isTrue();
	}

	@Test
	public void should_not_equal_when_values_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "df");

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.equals(proxy2)).isFalse();
	}

	@Test
	public void should_not_equal_when_one_value_null() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.equals(proxy2)).isFalse();
	}

	@Test
	public void should_equal_compare_key_when_both_values_null() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, null);
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.equals(proxy2)).isTrue();
	}

	@Test
	public void should_not_equal_when_keys_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, null);
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.equals(proxy2)).isFalse();
	}

	@Test
	public void should_same_hashcode_when_same_keys_and_values() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.hashCode()).isEqualTo(proxy2.hashCode());
	}

	@Test
	public void should_different_hashcode_when_values_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.hashCode()).isNotEqualTo(proxy2.hashCode());
	}

	@Test
	public void should_different_hashcode_when_keys_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

		MapEntryProxy<Integer, String> proxy1 = new MapEntryProxy<Integer, String>(entry1);
		MapEntryProxy<Integer, String> proxy2 = new MapEntryProxy<Integer, String>(entry2);

		assertThat(proxy1.hashCode()).isNotEqualTo(proxy2.hashCode());
	}
}
