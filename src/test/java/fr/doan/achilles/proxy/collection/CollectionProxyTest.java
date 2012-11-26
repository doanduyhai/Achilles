package fr.doan.achilles.proxy.collection;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;

@RunWith(MockitoJUnitRunner.class)
public class CollectionProxyTest
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
	public void should_mark_dirty_on_element_add() throws Exception
	{

		ListProxy<String> listProxy = prepareListProxy(new ArrayList<String>());
		listProxy.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception
	{

		ListProxy<String> listProxy = prepareListProxy(new ArrayList<String>());
		listProxy.addAll(Arrays.asList("a", "b"));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception
	{

		ListProxy<String> listProxy = prepareListProxy(new ArrayList<String>());
		listProxy.addAll(new ArrayList<String>());

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.clear();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception
	{

		ListProxy<String> listProxy = prepareListProxy(new ArrayList<String>());
		listProxy.clear();

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.remove("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_when_no_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.remove("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.removeAll(Arrays.asList("a", "c"));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_all_when_no_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.removeAll(Arrays.asList("d", "e"));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_retain_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.retainAll(Arrays.asList("a", "c"));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_retain_all_when_all_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.retainAll(Arrays.asList("a", "b", "c"));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	private ListProxy<String> prepareListProxy(List<String> target)
	{
		ListProxy<String> listProxy = new ListProxy<String>(target);
		listProxy.setDirtyMap(dirtyMap);
		listProxy.setSetter(setter);
		listProxy.setPropertyMeta(propertyMeta);

		return listProxy;
	}
}
