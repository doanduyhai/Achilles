package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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

	@Mock
	private EntityIntrospector introspector;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
	}

	@Test
	public void should_mark_dirty_on_element_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(introspector.unproxy("a")).thenReturn("a");
		wrapper.add("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_element_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(introspector.unproxy("a")).thenReturn("a");
		when(dirtyMap.containsKey(setter)).thenReturn(true);
		wrapper.add("a");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_false_when_adding_to_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.add("a")).isFalse();
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setHelper(new EntityIntrospector());

		wrapper.addAll(Arrays.asList("a", "b"));

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_false_when_adding_all_to_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.addAll(Arrays.asList("a", "b"))).isFalse();
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.addAll(new ArrayList<String>());

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_do_nothing_when_clearing_on_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		wrapper.setDirtyMap(dirtyMap);

		wrapper.clear();

		verifyZeroInteractions(dirtyMap);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_true_on_contains() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(Arrays.asList("a", "b"));
		when(introspector.unproxy("a")).thenReturn("a");
		assertThat(wrapper.contains("a")).isTrue();
	}

	@Test
	public void should_return_false_on_contains_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.contains("a")).isFalse();
	}

	@Test
	public void should_return_true_on_contains_all() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(Arrays.asList("a", "b", "c", "d"));

		List<String> check = Arrays.asList("a", "c");
		when(introspector.unproxy(check)).thenReturn(check);
		assertThat(wrapper.containsAll(check)).isTrue();
	}

	@Test
	public void should_return_false_on_contains_all_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.containsAll(Arrays.asList("a", "c"))).isFalse();
	}

	@Test
	public void should_return_true_on_empty_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(new ArrayList<String>());
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_return_true_on_empty_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_retyrb_null_on_iterator_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.iterator()).isNull();
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(introspector.unproxy("a")).thenReturn("a");
		wrapper.remove("a");

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
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.remove("c");

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_false_on_remove_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.remove("a")).isFalse();
		verifyZeroInteractions(dirtyMap);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setHelper(new EntityIntrospector());
		wrapper.removeAll(Arrays.asList("a", "c"));

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
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.removeAll(Arrays.asList("d", "e"));

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_false_on_remove_all_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.removeAll(Arrays.asList("a", "b"))).isFalse();
		verifyZeroInteractions(dirtyMap);
	}

	@Test
	public void should_mark_dirty_on_retain_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setHelper(new EntityIntrospector());
		wrapper.retainAll(Arrays.asList("a", "c"));

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
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setHelper(new EntityIntrospector());
		wrapper.retainAll(Arrays.asList("a", "b", "c"));

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_false_on_retain_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.retainAll(Arrays.asList("a", "b"))).isFalse();
		verifyZeroInteractions(dirtyMap);
	}

	@Test
	public void should_mark_dirty_on_iterator_remove() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		Iterator<String> iteratorWrapper = wrapper.iterator();

		assertThat(iteratorWrapper).isInstanceOf(IteratorWrapper.class);

		iteratorWrapper.next();
		iteratorWrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_size() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		assertThat(wrapper.size()).isEqualTo(3);
	}

	@Test
	public void should_return_zero_size_when_null_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.size()).isEqualTo(0);
	}

	@Test
	public void should_return_array_for_join() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when(introspector.buildProxy("a", joinMeta)).thenReturn("a");
		when(introspector.buildProxy("b", joinMeta)).thenReturn("b");
		when(introspector.buildProxy("c", joinMeta)).thenReturn("c");

		assertThat(wrapper.toArray()).contains("a", "b", "c");
	}

	@Test
	public void should_return_array() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray()).contains("a", "b", "c");
	}

	@Test
	public void should_return_null_array_when_null_target() throws Exception
	{
		Collection<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.toArray()).isNull();
	}

	@Test
	public void should_return_array_with_argument_for_join() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when(introspector.buildProxy("a", joinMeta)).thenReturn("a");
		when(introspector.buildProxy("b", joinMeta)).thenReturn("b");
		when(introspector.buildProxy("c", joinMeta)).thenReturn("c");

		assertThat(wrapper.toArray(new String[]
		{
				"a",
				"c"
		})).contains("a", "c");
	}

	@Test
	public void should_return_array_with_argument() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray(new String[]
		{
				"a",
				"c"
		})).contains("a", "c");
	}

	@Test
	public void should_return_null_array_with_argument_when_null_target() throws Exception
	{
		Collection<String> wrapper = prepareListWrapperWithNull();
		assertThat(wrapper.toArray(new String[]
		{
				"a",
				"c"
		})).isNull();
	}

	@Test
	public void should_return_target() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		CollectionWrapper<String> wrapper = new CollectionWrapper<String>(target);
		assertThat(wrapper.getTarget()).isSameAs(target);
	}

	@Test
	public void should_return_null_target() throws Exception
	{
		CollectionWrapper<String> wrapper = new CollectionWrapper<String>(null);
		assertThat(wrapper.getTarget()).isNull();
	}

	@Test
	public void should_return_null_when_no_join_meta() throws Exception
	{
		CollectionWrapper<String> wrapper = prepareListWrapper(null);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.joinMeta()).isNull();
	}

	private ListWrapper<String> prepareListWrapper(List<String> target)
	{
		ListWrapper<String> wrapper = new ListWrapper<String>(target);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setHelper(introspector);
		return wrapper;
	}

	private ListWrapper<String> prepareListWrapperWithNull()
	{
		return new ListWrapper<String>(null);
	}
}
