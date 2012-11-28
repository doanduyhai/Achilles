package fr.doan.achilles.wrapper;

import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.ArrayList;
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
public class ListIteratorProxyTest
{

	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer> propertyMeta;

	ListIteratorProxy<Integer> proxy;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);

		List<Integer> list = new ArrayList<Integer>();
		list.add(1);
		list.add(2);

		proxy = new ListIteratorProxy<Integer>(list.listIterator());
		proxy.setDirtyMap(dirtyMap);
		proxy.setSetter(setter);
		proxy.setPropertyMeta(propertyMeta);

	}

	@Test
	public void should_mark_dirty_on_add() throws Exception
	{
		proxy.add(3);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_set() throws Exception
	{
		proxy.next();
		proxy.set(1);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		proxy.next();
		proxy.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
