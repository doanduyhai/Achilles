/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.proxy.wrapper;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.APPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.PREPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.google.common.collect.Lists;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListWrapperTest {

	private Map<Method, DirtyChecker> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private EntityProxifier proxifier;


	@Before
	public void setUp() throws Exception {
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        dirtyMap = new HashMap<>();
	}

    @Test
    public void should_mark_dirty_on_element_add() throws Exception {
        List<Object> target = new ArrayList<>();
        ListWrapper wrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.add("a");

        assertThat(target).containsExactly("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(APPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsExactly("a");
    }

    @Test
    public void should_mark_dirty_on_add_all() throws Exception {

        List<Object> target = new ArrayList<>();
        ListWrapper wrapper = prepareListWrapper(target);
        Collection<String> list = asList("a", "b");

        wrapper.setProxifier(proxifier);

        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.addAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(APPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a", "b");
    }

    @Test
    public void should_not_mark_dirty_on_empty_add_all() throws Exception {

        List<Object> target = new ArrayList<>();
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.addAll(new HashSet<>());

        assertThat(target).hasSize(0);

        assertThat(dirtyMap).isEmpty();
    }

    @Test
	public void should_mark_dirty_on_element_prepend() throws Exception {

		ArrayList<Object> target = new ArrayList<>();
		ListWrapper listWrapper = prepareListWrapper(target);
		when(proxifier.removeProxy("a")).thenReturn("a");
		listWrapper.add(0, "a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(PREPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a");
	}

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_adding_at_index() throws Exception {
        //Given
        ArrayList<Object> target = new ArrayList<>();
        ListWrapper listWrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");

        //When
        listWrapper.add(1, "a");
    }

	@Test
	public void should_mark_dirty_on_preprend_all() throws Exception {

		ArrayList<Object> target = new ArrayList<>();
		ListWrapper listWrapper = prepareListWrapper(target);
		listWrapper.setProxifier(proxifier);

		Collection<String> list = asList("b", "c");
		when(proxifier.removeProxy(list)).thenReturn(list);

		listWrapper.addAll(0, list);

		assertThat(target).containsExactly("b","c");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(PREPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsExactly("b", "c");
	}

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_adding_all_at_index() throws Exception {
        //Given
        ArrayList<Object> target = new ArrayList<>();
        ListWrapper listWrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");

        //When
        listWrapper.addAll(1, asList("a"));
    }

    @Test
    public void should_mark_dirty_on_clear() throws Exception {
        List<Object> target = Lists.<Object>newArrayList("a");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.clear();

        assertThat(target).hasSize(0);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_COLLECTION_OR_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).isEmpty();
    }

    @Test
    public void should_not_mark_dirty_on_clear_when_empty() throws Exception {
        List<Object> target = new ArrayList<>();
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.clear();

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_get_element_at_index() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a","b","c");
        ListWrapper wrapper = prepareListWrapper(target);

        //Then
        assertThat(wrapper.get(1)).isEqualTo("b");
    }
    @Test
    public void should_return_index_of() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b");
        ListWrapper listWrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("b")).thenReturn("b");

        //Then
        assertThat(listWrapper.indexOf("b")).isEqualTo(1);
    }

    @Test
    public void should_return_last_index_of() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b", "a");
        ListWrapper listWrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");

        //Then
        assertThat(listWrapper.lastIndexOf("a")).isEqualTo(2);
    }

    @Test
    public void should_return_copy_of_list_iterator() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper listWrapper = prepareListWrapper(target);

        //When
        ListIterator<Object> listIterator = listWrapper.listIterator();
        listIterator.next();
        listIterator.remove();

        //Then
        assertThat(target).containsExactly("a", "b", "c");
    }

    @Test
    public void should_return_of_list_iterator_at_index() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b", "c", "d");
        ListWrapper listWrapper = prepareListWrapper(target);

        //When
        ListIterator<Object> listIterator = listWrapper.listIterator(1);
        listIterator.next();
        listIterator.remove();

        //Then
        assertThat(target).containsExactly("a", "b", "c", "d");
    }

    @Test
    public void should_return_true_for_contains() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper listWrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");

        //Then
        assertThat(listWrapper.contains("a")).isTrue();
    }

    @Test
    public void should_return_true_for_contains_all() throws Exception {
        //Given
        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper listWrapper = prepareListWrapper(target);

        //Then
        assertThat(listWrapper.containsAll(asList("a","c"))).isTrue();
    }

    @Test
    public void should_return_true_on_is_empty() throws Exception {
        //Given
        List<Object> target = asList();
        ListWrapper listWrapper = prepareListWrapper(target);

        //Then
        assertThat(listWrapper.isEmpty()).isTrue();
    }

    @Test
    public void should_return_copy_of_iterator() throws Exception {
        //Given
        List<Object> target = new ArrayList<>();
        target.add("a");
        target.add("b");

        ListWrapper listWrapper = prepareListWrapper(target);

        //When
        Iterator<Object> iterator = listWrapper.iterator();
        iterator.next();
        iterator.remove();

        //Then
        assertThat(target).containsExactly("a","b");
    }

    @Test
    public void should_mark_dirty_on_remove() throws Exception {
        List<Object> target = Lists.<Object>newArrayList("a", "b");
        ListWrapper wrapper = prepareListWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.remove("a");

        assertThat(target).containsExactly("b");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a");
    }

    @Test
    public void should_not_mark_dirty_on_remove_when_no_match() throws Exception {

        List<Object> target = Lists.<Object>newArrayList("a", "b");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.remove("c");

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_remove_all() throws Exception {

        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.setProxifier(proxifier);

        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.removeAll(list);

        assertThat(target).containsExactly("b");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a", "c");
    }

    @Test
    public void should_not_mark_dirty_on_remove_all_when_no_match() throws Exception {

        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.removeAll(Arrays.asList("d", "e"));

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_retain_all() throws Exception {

        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.setProxifier(proxifier);
        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.retainAll(list);

        assertThat(target).containsOnly("a", "c");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("b");
    }

    @Test
    public void should_not_mark_dirty_on_retain_all_when_all_match() throws Exception {

        List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
        ListWrapper wrapper = prepareListWrapper(target);
        wrapper.setProxifier(proxifier);
        Collection<String> list = Arrays.asList("a", "b", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.retainAll(list);

        assertThat(target).containsOnly("a", "b", "c");

        assertThat(dirtyMap).isEmpty();
    }

    @Test
	public void should_mark_dirty_on_remove_at_index() throws Exception {

		List<Object> target = Lists.<Object>newArrayList("a", "b");
		ListWrapper listWrapper = prepareListWrapper(target);
		listWrapper.remove(1);

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);

	}

	@Test
	public void should_mark_dirty_on_set() throws Exception {

		List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
		ListWrapper listWrapper = prepareListWrapper(target);
		when(proxifier.removeProxy("d")).thenReturn("d");
		listWrapper.set(1, "d");

		assertThat(target).hasSize(3);
		assertThat(target.get(1)).isEqualTo("d");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
	}

	@Test
	public void should_not_mark_dirty_on_list_iterator_add() throws Exception {
		List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
		ListIterator<Object> listIteratorWrapper = prepareListWrapper(target).listIterator();

		when(proxifier.removeProxy("c")).thenReturn("c");
		listIteratorWrapper.add("c");

		assertThat(dirtyMap).isEmpty();
	}

	@Test
	public void should_not_mark_dirty_on_sub_list_add() throws Exception {
		List<Object> target = Lists.<Object>newArrayList("a", "b", "c");
		List<Object> subListWrapper = prepareListWrapper(target).subList(0, 1);

		when(proxifier.removeProxy("d")).thenReturn("d");
		subListWrapper.add("d");

        assertThat(dirtyMap).isEmpty();
	}

	@Test
	public void should_get_target() throws Exception {
		ArrayList<Object> target = new ArrayList<>();
		ListWrapper listWrapper = prepareListWrapper(target);

		assertThat(listWrapper.getTarget()).isSameAs(target);
	}

	private ListWrapper prepareListWrapper(List<Object> target) {
		ListWrapper listWrapper = new ListWrapper(target);
		listWrapper.setDirtyMap(dirtyMap);
		listWrapper.setSetter(setter);
		listWrapper.setPropertyMeta(propertyMeta);
		listWrapper.setProxifier(proxifier);
		return listWrapper;
	}
}
