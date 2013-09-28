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
package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectionWrapperTest {
	@Mock
	private Map<Method, PropertyMeta> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Mock
	private PersistenceContext context;

	private EntityMeta entityMeta;

	@Before
	public void setUp() throws Exception {
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).accessors().build();

		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
	}

	@Test
	public void should_mark_dirty_on_element_add() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		ListWrapper wrapper = prepareListWrapper(target);
		when(proxifier.unwrap("a")).thenReturn("a");
		wrapper.add("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_element_add() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		ListWrapper wrapper = prepareListWrapper(target);
		when(proxifier.unwrap("a")).thenReturn("a");
		when(dirtyMap.containsKey(setter)).thenReturn(true);
		wrapper.add("a");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		ListWrapper wrapper = prepareListWrapper(target);
		Collection<String> list = Arrays.asList("a", "b");

		wrapper.setProxifier(proxifier);

		when(proxifier.unwrap(Mockito.<Collection<String>> any())).thenReturn(
				list);

		wrapper.addAll(list);

		verify(proxifier).unwrap(list);

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.addAll(new ArrayList<Object>());

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_true_on_contains() throws Exception {
		ListWrapper wrapper = prepareListWrapper(Arrays.<Object> asList("a",
				"b"));
		when(proxifier.unwrap("a")).thenReturn("a");
		assertThat(wrapper.contains("a")).isTrue();
	}

	@Test
	public void should_return_true_on_contains_all() throws Exception {
		ListWrapper wrapper = prepareListWrapper(Arrays.<Object> asList("a",
				"b", "c", "d"));

		List<Object> check = Arrays.<Object> asList("a", "c");
		when(proxifier.unwrap(check)).thenReturn(check);
		assertThat(wrapper.containsAll(check)).isTrue();
	}

	@Test
	public void should_return_true_on_empty_target() throws Exception {
		ListWrapper wrapper = prepareListWrapper(new ArrayList<Object>());
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		ListWrapper wrapper = prepareListWrapper(target);
		when(proxifier.unwrap("a")).thenReturn("a");
		wrapper.remove("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_when_no_match()
			throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.remove("c");

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception {

		List<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);

		Collection<String> list = Arrays.asList("a", "c");
		when(proxifier.unwrap(Mockito.<Collection<String>> any())).thenReturn(
				list);

		wrapper.removeAll(list);

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_all_when_no_match()
			throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.removeAll(Arrays.asList("d", "e"));

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_retain_all() throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);
		Collection<String> list = Arrays.asList("a", "c");
		when(proxifier.unwrap(Mockito.<Collection<String>> any())).thenReturn(
				list);

		wrapper.retainAll(list);

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("c");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_retain_all_when_all_match()
			throws Exception {

		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);
		Collection<String> list = Arrays.asList("a", "b", "c");
		when(proxifier.unwrap(Mockito.<Collection<String>> any())).thenReturn(
				list);

		wrapper.retainAll(list);

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_iterator_remove() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);

		Iterator<Object> iteratorWrapper = wrapper.iterator();

		assertThat(iteratorWrapper).isInstanceOf(IteratorWrapper.class);

		iteratorWrapper.next();
		iteratorWrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_size() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);
		assertThat(wrapper.size()).isEqualTo(3);
	}

	@Test
	public void should_return_array() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray()).contains("a", "b", "c");
	}

	@Test
	public void should_return_array_with_argument() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray(new String[] { "a", "c" })).contains("a",
				"c");
	}

	@Test
	public void should_return_target() throws Exception {
		ArrayList<Object> target = new ArrayList<Object>();
		target.add("a");
		CollectionWrapper wrapper = new CollectionWrapper(target);
		assertThat(wrapper.getTarget()).isSameAs(target);
	}

	private ListWrapper prepareListWrapper(List<Object> target) {
		ListWrapper wrapper = new ListWrapper(target);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		wrapper.setContext(context);
		return wrapper;
	}
}
