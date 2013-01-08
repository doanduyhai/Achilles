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

/**
 * CollectionWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectionWrapperTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_mark_dirty_on_element_add() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.add("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.addAll(Arrays.asList("a", "b"));

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.addAll(new ArrayList<String>());

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.remove("a");

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.remove("c");

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.removeAll(Arrays.asList("a", "c"));

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.removeAll(Arrays.asList("d", "e"));

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.retainAll(Arrays.asList("a", "c"));

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);
		listWrapper.retainAll(Arrays.asList("a", "b", "c"));

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
		ListWrapper<String> listWrapper = prepareListWrapper(target);

		Iterator<String> iteratorWrapper = listWrapper.iterator();

		assertThat(iteratorWrapper).isInstanceOf(IteratorWrapper.class);

		iteratorWrapper.next();
		iteratorWrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	private ListWrapper<String> prepareListWrapper(List<String> target)
	{
		ListWrapper<String> listWrapper = new ListWrapper<String>(target);
		listWrapper.setDirtyMap(dirtyMap);
		listWrapper.setSetter(setter);
		listWrapper.setPropertyMeta(propertyMeta);

		return listWrapper;
	}
}
