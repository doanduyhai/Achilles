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
public class IteratorProxyTest
{
	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_mark_dirty_on_element_remove() throws Exception
	{

		List<Integer> list = new ArrayList<Integer>();
		list.add(1);
		list.add(2);

		IteratorProxy<Integer> proxy = new IteratorProxy<Integer>(list.iterator());
		proxy.setDirtyMap(dirtyMap);
		proxy.setSetter(setter);
		proxy.setPropertyMeta(propertyMeta);

		proxy.next();
		proxy.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
