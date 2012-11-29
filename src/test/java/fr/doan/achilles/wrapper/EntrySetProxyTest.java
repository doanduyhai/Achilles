package fr.doan.achilles.wrapper;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

@RunWith(MockitoJUnitRunner.class)
public class EntrySetProxyTest
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
	public void should_mark_dirty_on_clear() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		proxy.clear();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_iterator_remove() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Iterator<Entry<Integer, String>> iterator = proxy.iterator();

		iterator.next();
		iterator.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Entry<Integer, String> entry = map.entrySet().iterator().next();

		proxy.remove(entry);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_external_element() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);
		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

		proxy.remove(entry);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_mark_dirty_on_remove_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();

		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();

		proxy.removeAll(Arrays.asList(entry1, entry2));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_not_mark_dirty_on_remove_all_not_matching() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(5, "csdf");

		proxy.removeAll(Arrays.asList(entry1, entry2));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_mark_dirty_on_retain_one() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();

		Entry<Integer, String> entry1 = iterator.next();

		proxy.retainAll(Arrays.asList(entry1));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_not_mark_dirty_on_retain_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();

		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();
		Entry<Integer, String> entry3 = iterator.next();

		proxy.retainAll(Arrays.asList(entry1, entry2, entry3));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

		proxy.add(entry);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetProxy<Integer, String> proxy = prepareProxy(map);

		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(5, "csdf");

		proxy.addAll(Arrays.asList(entry1, entry2));
	}

	private EntrySetProxy<Integer, String> prepareProxy(Map<Integer, String> map)
	{
		EntrySetProxy<Integer, String> proxy = new EntrySetProxy<Integer, String>(map.entrySet());

		proxy.setDirtyMap(dirtyMap);
		proxy.setSetter(setter);
		proxy.setPropertyMeta(propertyMeta);

		return proxy;
	}

}
