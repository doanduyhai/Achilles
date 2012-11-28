package fr.doan.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

		ArrayList<String> target = new ArrayList<String>();
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.add("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.addAll(Arrays.asList("a", "b"));

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.addAll(new ArrayList<String>());

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.clear();

		assertThat(target).hasSize(0);

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

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

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

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

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

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

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

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

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

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("c");

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

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_iterator_remove() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);

		Iterator<String> iteratorProxy = listProxy.iterator();

		assertThat(iteratorProxy).isInstanceOf(IteratorProxy.class);

		iteratorProxy.next();
		iteratorProxy.remove();

		verify(dirtyMap).put(setter, propertyMeta);
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
