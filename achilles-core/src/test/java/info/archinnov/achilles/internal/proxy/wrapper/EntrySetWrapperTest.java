/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.internal.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
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

@RunWith(MockitoJUnitRunner.class)
public class EntrySetWrapperTest {
	@Mock
	private Map<Method, PropertyMeta> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private PersistenceContext context;

	private EntityMeta entityMeta;

	@Before
	public void setUp() throws Exception {
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).accessors().build();

		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		wrapper.clear();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_true_on_contains() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);
		Entry<Object, Object> entry = map.entrySet().iterator().next();
		when(proxifier.removeProxy(any())).thenReturn(entry);

		assertThat(wrapper.contains(entry)).isTrue();
	}

	@Test
	public void should_return_true_on_containsAll() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);
		Iterator<Entry<Object, Object>> iterator = map.entrySet().iterator();

		Entry<Object, Object> entry1 = iterator.next();
		Entry<Object, Object> entry2 = iterator.next();

		when(proxifier.removeProxy(entry1)).thenReturn(entry1);
		when(proxifier.removeProxy(entry2)).thenReturn(entry2);

		assertThat(wrapper.containsAll(Arrays.asList(entry1, entry2))).isTrue();
	}

	@Test
	public void should_return_true_on_isEmpty() throws Exception {
		EntrySetWrapper wrapper = prepareWrapper(new HashMap<Object, Object>());
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_return_iterator() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		assertThat(wrapper.iterator()).isNotNull();
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);
		Entry<Object, Object> entry = map.entrySet().iterator().next();
		when(proxifier.removeProxy(any())).thenReturn(entry);
		wrapper.remove(entry);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_external_element() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);
		Entry<Object, Object> entry = new AbstractMap.SimpleEntry<Object, Object>(4, "csdf");
		wrapper.remove(entry);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		Iterator<Entry<Object, Object>> iterator = map.entrySet().iterator();

		Entry<Object, Object> entry1 = iterator.next();
		Entry<Object, Object> entry2 = iterator.next();
		List<Entry<Object, Object>> list = new ArrayList<Entry<Object, Object>>();
		list.add(entry1);
		list.add(entry2);

		when(proxifier.removeProxy((Collection<Entry<Object, Object>>) list)).thenReturn(list);

		wrapper.removeAll(list);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_all_not_matching() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		Entry<Object, Object> entry1 = new AbstractMap.SimpleEntry<Object, Object>(4, "csdf");
		Entry<Object, Object> entry2 = new AbstractMap.SimpleEntry<Object, Object>(5, "csdf");

		wrapper.removeAll(Arrays.asList(entry1, entry2));

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_retain_all() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		Iterator<Entry<Object, Object>> iterator = map.entrySet().iterator();
		Entry<Object, Object> entry1 = iterator.next();
		Entry<Object, Object> entry2 = iterator.next();
		List<Entry<Object, Object>> list = Arrays.asList(entry1, entry2);

		when(proxifier.removeProxy((Collection<Entry<Object, Object>>) list)).thenReturn(list);

		EntrySetWrapper wrapper = prepareWrapper(map);
		wrapper.retainAll(list);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_retain_all_no_dirty_when_all_match() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");

		Entry<Object, Object> entry1 = new AbstractMap.SimpleEntry<Object, Object>(1, "FR");
		List<Entry<Object, Object>> list = Arrays.asList(entry1);
		when(proxifier.removeProxy((Collection<Entry<Object, Object>>) list)).thenReturn(list);
		EntrySetWrapper wrapper = prepareWrapper(map);

		wrapper.retainAll(list);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_get_size() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		EntrySetWrapper wrapper = prepareWrapper(map);
		assertThat(wrapper.size()).isEqualTo(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_array() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		EntrySetWrapper wrapper = prepareWrapper(map);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		Object[] array = wrapper.toArray();

		assertThat(array).hasSize(1);
		assertThat(array[0]).isInstanceOf(Entry.class);
		assertThat(((Entry<Object, Object>) array[0]).getValue()).isEqualTo("FR");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_array_with_argument() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		Entry<Object, Object> entry = map.entrySet().iterator().next();

		EntrySetWrapper wrapper = prepareWrapper(map);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		Object[] array = wrapper.toArray(new Entry[] { entry });

		assertThat(array).hasSize(1);
		assertThat(((Entry<Object, Object>) array[0]).getValue()).isEqualTo("FR");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		Entry<Object, Object> entry = new AbstractMap.SimpleEntry<Object, Object>(4, "csdf");

		wrapper.add(entry);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_add_all() throws Exception {
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		EntrySetWrapper wrapper = prepareWrapper(map);

		Entry<Object, Object> entry1 = new AbstractMap.SimpleEntry<Object, Object>(4, "csdf");
		Entry<Object, Object> entry2 = new AbstractMap.SimpleEntry<Object, Object>(5, "csdf");

		wrapper.addAll(Arrays.asList(entry1, entry2));
	}

	private EntrySetWrapper prepareWrapper(Map<Object, Object> map) {
		EntrySetWrapper wrapper = new EntrySetWrapper(map.entrySet());
		wrapper.setContext(context);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		return wrapper;
	}
}
