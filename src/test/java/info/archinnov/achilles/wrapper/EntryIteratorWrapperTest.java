package info.archinnov.achilles.wrapper;

import static org.mockito.Mockito.verify;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.EntryIteratorWrapper;

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


/**
 * EntryIteratorWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntryIteratorWrapperTest
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
	public void should_mark_dirty_on_element_remove() throws Exception
	{

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntryIteratorWrapper<Integer, String> wrapper = new EntryIteratorWrapper<Integer, String>(
				map.entrySet().iterator());
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);

		wrapper.next();
		wrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);

	}

}
