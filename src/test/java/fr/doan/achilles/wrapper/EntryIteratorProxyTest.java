package fr.doan.achilles.wrapper;

import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.HashMap;
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
public class EntryIteratorProxyTest
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
	public void should_mark_dirty_on_element_remove() throws Exception
	{

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntryIteratorProxy<Integer, String> proxy = new EntryIteratorProxy<Integer, String>(map.entrySet().iterator());
		proxy.setDirtyMap(dirtyMap);
		proxy.setSetter(setter);
		proxy.setPropertyMeta(propertyMeta);

		proxy.next();
		proxy.remove();

		verify(dirtyMap).put(setter, propertyMeta);

	}

}
