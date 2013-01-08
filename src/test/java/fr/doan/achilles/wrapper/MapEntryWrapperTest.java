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

/**
 * MapEntryWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MapEntryWrapperTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

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

		MapEntryWrapper<Integer, String> mapEntryWrapper = new MapEntryWrapper<Integer, String>(
				mapEntry);

		mapEntryWrapper.setDirtyMap(dirtyMap);
		mapEntryWrapper.setSetter(setter);
		mapEntryWrapper.setPropertyMeta(propertyMeta);

		mapEntryWrapper.setValue("TEST");

		verify(dirtyMap).put(setter, propertyMeta);

	}

	@Test
	public void should_equal() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.equals(wrapper2)).isTrue();
	}

	@Test
	public void should_not_equal_when_values_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "df");

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.equals(wrapper2)).isFalse();
	}

	@Test
	public void should_not_equal_when_one_value_null() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.equals(wrapper2)).isFalse();
	}

	@Test
	public void should_equal_compare_key_when_both_values_null() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, null);
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.equals(wrapper2)).isTrue();
	}

	@Test
	public void should_not_equal_when_keys_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, null);
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.equals(wrapper2)).isFalse();
	}

	@Test
	public void should_same_hashcode_when_same_keys_and_values() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.hashCode()).isEqualTo(wrapper2.hashCode());
	}

	@Test
	public void should_different_hashcode_when_values_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.hashCode()).isNotEqualTo(wrapper2.hashCode());
	}

	@Test
	public void should_different_hashcode_when_keys_differ() throws Exception
	{
		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, "abc");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

		MapEntryWrapper<Integer, String> wrapper1 = new MapEntryWrapper<Integer, String>(entry1);
		MapEntryWrapper<Integer, String> wrapper2 = new MapEntryWrapper<Integer, String>(entry2);

		assertThat(wrapper1.hashCode()).isNotEqualTo(wrapper2.hashCode());
	}
}
