package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * MapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MapWrapperTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private EntityHelper helper;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
		when(propertyMeta.type()).thenReturn(PropertyType.MAP);
	}

	@Test
	public void should_mark_dirty_when_clear_on_full_map() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		wrapper.clear();

		assertThat(target).isEmpty();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_when_clear_on_empty_map() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		target.clear();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		wrapper.clear();

		verifyZeroInteractions(dirtyMap);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_new_entry_in_entrySet() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Set<Entry<Integer, String>> entrySet = wrapper.entrySet();

		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		entrySet.add(entry);
	}

	@Test
	public void should_mark_dirty_on_remove_from_entrySet() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Set<Entry<Integer, String>> entrySet = wrapper.entrySet();

		Entry<Integer, String> entry = target.entrySet().iterator().next();
		when(helper.unproxy(entry)).thenReturn(entry);
		entrySet.remove(entry);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_non_existing_from_entrySet() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Set<Entry<Integer, String>> entrySet = wrapper.entrySet();

		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		entrySet.remove(entry);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_set_value_from_entry() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Set<Entry<Integer, String>> entrySet = wrapper.entrySet();

		entrySet.iterator().next().setValue("sdfsd");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_from_keySet() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Set<Integer> keySet = wrapper.keySet();
		when(helper.unproxy(1)).thenReturn(1);
		keySet.remove(1);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_from_keySet_iterator() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Iterator<Integer> keyIterator = wrapper.keySet().iterator();
		keyIterator.next();
		keyIterator.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_put() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		wrapper.put(4, "sdfs");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_put_all() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");

		wrapper.putAll(map);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_existing() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);
		when(helper.unproxy(1)).thenReturn(1);
		wrapper.remove(1);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_non_existing() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		wrapper.remove(10);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_collection_remove() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Collection<String> collectionWrapper = wrapper.values();
		when(helper.unproxy("FR")).thenReturn("FR");
		collectionWrapper.remove("FR");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	public void should_not_mark_dirty_on_collection_remove_non_existing() throws Exception
	{
		Map<Integer, String> target = prepareMap();
		MapWrapper<Integer, String> wrapper = prepareMapWrapper(target);

		Collection<String> collectionWrapper = wrapper.values();

		collectionWrapper.remove("sdfsdf");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	private Map<Integer, String> prepareMap()
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		return map;
	}

	private MapWrapper<Integer, String> prepareMapWrapper(Map<Integer, String> target)
	{
		MapWrapper<Integer, String> wrapper = new MapWrapper<Integer, String>(target);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setHelper(helper);
		return wrapper;
	}
}
