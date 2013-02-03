package info.archinnov.achilles.wrapper;

import static org.mockito.Mockito.verify;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.ListIteratorWrapper;

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


/**
 * ListIteratorWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ListIteratorWrapperTest
{

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, Integer> propertyMeta;

	ListIteratorWrapper<Integer> wrapper;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);

		List<Integer> list = new ArrayList<Integer>();
		list.add(1);
		list.add(2);

		wrapper = new ListIteratorWrapper<Integer>(list.listIterator());
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);

	}

	@Test
	public void should_mark_dirty_on_add() throws Exception
	{
		wrapper.add(3);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_set() throws Exception
	{
		wrapper.next();
		wrapper.set(1);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		wrapper.next();
		wrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
