package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ListWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ListWrapperTest
{

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Mock
	private AchillesEntityProxifier proxifier;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_mark_dirty_on_element_add_at_index() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		when(proxifier.unproxy("a")).thenReturn("a");
		listWrapper.add(0, "a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all_at_index() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.setProxifier(new ThriftEntityProxifier());
		listWrapper.addAll(1, Arrays.asList("b", "c"));

		assertThat(target).hasSize(3);
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_at_index() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.remove(1);

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_set() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		when(proxifier.unproxy("d")).thenReturn("d");
		listWrapper.set(1, "d");

		assertThat(target).hasSize(3);
		assertThat(target.get(1)).isEqualTo("d");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_list_iterator_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListIterator<String> listIteratorWrapper = prepareListWrapper(target).listIterator();

		assertThat(listIteratorWrapper).isInstanceOf(ListIteratorWrapper.class);
		when(proxifier.unproxy("c")).thenReturn("c");
		listIteratorWrapper.add("c");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_sub_list_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		List<String> subListWrapper = prepareListWrapper(target).subList(0, 1);

		assertThat(subListWrapper).isInstanceOf(ListWrapper.class);
		when(proxifier.unproxy("d")).thenReturn("d");
		subListWrapper.add("d");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_get_target() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);

		assertThat(listWrapper.getTarget()).isSameAs(target);
	}

	private ListWrapper<String> prepareListWrapper(List<String> target)
	{
		ListWrapper<String> listWrapper = new ListWrapper<String>(target);
		listWrapper.setDirtyMap(dirtyMap);
		listWrapper.setSetter(setter);
		listWrapper.setPropertyMeta(propertyMeta);
		listWrapper.setProxifier(proxifier);
		return listWrapper;
	}
}
