package fr.doan.achilles.proxy.collection;

import static org.fest.assertions.api.Assertions.assertThat;
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
public class ListProxyTest
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
	public void should_mark_dirty_on_element_add_at_index() throws Exception
	{

		ListProxy<String> listProxy = prepareListProxy(new ArrayList<String>());
		listProxy.add(0, "a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all_at_index() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.addAll(1, Arrays.asList("b", "c"));

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_at_index() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.remove(1);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_set() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListProxy<String> listProxy = prepareListProxy(target);
		listProxy.set(1, "d");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_get_target() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListProxy<String> listProxy = prepareListProxy(target);

		assertThat(listProxy.getTarget()).isSameAs(target);
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
