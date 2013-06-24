package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * AchillesEntrySetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntrySetWrapperTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private PropertyMeta<Integer, CompleteBean> joinPropertyMeta;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Mock
	private PersistenceContext context;

	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.accessors()
				.build();

		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		wrapper.clear();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_true_on_contains() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		Entry<Integer, String> entry = map.entrySet().iterator().next();
		when(proxifier.unwrap(any())).thenReturn(entry);

		assertThat(wrapper.contains(entry)).isTrue();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_true_on_containsAll() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();

		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();

		when(proxifier.unwrap(entry1)).thenReturn(entry1);
		when(proxifier.unwrap(entry2)).thenReturn(entry2);

		assertThat(wrapper.containsAll(Arrays.asList(entry1, entry2))).isTrue();
	}

	@Test
	public void should_return_true_on_isEmpty() throws Exception
	{
		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(new HashMap<Integer, String>());
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_return_iterator() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		assertThat(wrapper.iterator()).isNotNull();
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		Entry<Integer, String> entry = map.entrySet().iterator().next();
		when(proxifier.unwrap(any())).thenReturn(entry);
		wrapper.remove(entry);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_external_element() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		wrapper.remove(entry);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();

		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();
		List<Entry<Integer, String>> list = new ArrayList<Map.Entry<Integer, String>>();
		list.add(entry1);
		list.add(entry2);

		when(proxifier.unwrap((Collection<Entry<Integer, String>>) list)).thenReturn(list);

		wrapper.removeAll(list);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_not_mark_dirty_on_remove_all_not_matching() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(5, "csdf");

		wrapper.removeAll(Arrays.asList(entry1, entry2));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_retain_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		Iterator<Entry<Integer, String>> iterator = map.entrySet().iterator();
		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();
		List<Entry<Integer, String>> list = Arrays.asList(entry1, entry2);

		when(proxifier.unwrap((Collection<Entry<Integer, String>>) list)).thenReturn(list);

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		wrapper.retainAll(list);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_retain_all_no_dirty_when_all_match() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");

		Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, "FR");
		List<Entry<Integer, String>> list = Arrays.asList(entry1);
		when(proxifier.unwrap((Collection<Entry<Integer, String>>) list)).thenReturn(list);
		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		wrapper.retainAll(list);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_get_size() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		assertThat(wrapper.size()).isEqualTo(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_array() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		Object[] array = wrapper.toArray();

		assertThat(array).hasSize(1);
		assertThat(array[0]).isInstanceOf(Map.Entry.class);
		assertThat(((Entry<Integer, String>) array[0]).getValue()).isEqualTo("FR");
	}

	@Test
	public void should_return_array_when_join() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SET);

		Object[] array = wrapper.toArray();

		assertThat(array).hasSize(1);
		assertThat(array[0]).isInstanceOf(MapEntryWrapper.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_array_with_argument() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		Entry<Integer, String> entry = map.entrySet().iterator().next();

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		Object[] array = wrapper.toArray(new Entry[]
		{
			entry
		});

		assertThat(array).hasSize(1);
		assertThat(((Entry<Integer, String>) array[0]).getValue()).isEqualTo("FR");
	}

	@Test
	public void should_return_array_with_argument_when_join_entity() throws Exception
	{
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, entity);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		EntrySetWrapper<Integer, CompleteBean> wrapper = prepareJoinWrapper(map);
		when(joinPropertyMeta.type()).thenReturn(PropertyType.JOIN_SET);

		when(joinPropertyMeta.joinMeta()).thenReturn(entityMeta);
		when(proxifier.buildProxy(eq(entity), any(PersistenceContext.class))).thenReturn(
				entity);

		Object[] array = wrapper.toArray(new Entry[]
		{
			entry
		});

		assertThat(array).hasSize(1);
		assertThat(((Entry<Integer, CompleteBean>) array[0]).getValue()).isEqualTo(entity);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		Map.Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

		wrapper.add(entry);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add_all() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper<Integer, String> wrapper = prepareWrapper(map);

		Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
		Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(5, "csdf");

		wrapper.addAll(Arrays.asList(entry1, entry2));
	}

	private EntrySetWrapper<Integer, String> prepareWrapper(Map<Integer, String> map)
	{
		EntrySetWrapper<Integer, String> wrapper = new EntrySetWrapper<Integer, String>(
				map.entrySet());
		wrapper.setContext(context);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		return wrapper;
	}

	private EntrySetWrapper<Integer, CompleteBean> prepareJoinWrapper(
			Map<Integer, CompleteBean> map)
	{
		EntrySetWrapper<Integer, CompleteBean> wrapper = new EntrySetWrapper<Integer, CompleteBean>(
				map.entrySet());
		wrapper.setContext(context);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(joinPropertyMeta);
		wrapper.setProxifier(proxifier);
		return wrapper;
	}
}
