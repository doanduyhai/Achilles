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

import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_SET;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SetWrapperTest {

    private Map<Method, DirtyChecker> dirtyMap;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private EntityProxifier proxifier;

    private EntityMeta entityMeta;

    @Before
    public void setUp() throws Exception {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        when(propertyMeta.type()).thenReturn(PropertyType.LIST);

        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.SIMPLE).accessors().build();

        entityMeta = new EntityMeta();
//        entityMeta.setIdMeta(idMeta);
        dirtyMap = new HashMap<>();
    }

    @Test
    public void should_mark_dirty_on_element_add() throws Exception {
        Set<Object> target = new HashSet<>();
        SetWrapper wrapper = prepareSetWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.add("a");

        assertThat(target).containsExactly("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsExactly("a");
    }

    @Test
    public void should_not_mark_dirty_on_element_add() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a");
        SetWrapper wrapper = prepareSetWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.add("a");

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_add_all() throws Exception {

        Set<Object> target = new HashSet<>();
        SetWrapper wrapper = prepareSetWrapper(target);
        Collection<String> list = Arrays.asList("a", "b");

        wrapper.setProxifier(proxifier);

        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.addAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a", "b");
    }

    @Test
    public void should_not_mark_dirty_on_empty_add_all() throws Exception {

        Set<Object> target = new HashSet<>();
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.addAll(new HashSet<>());

        assertThat(target).hasSize(0);

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_clear() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.clear();

        assertThat(target).hasSize(0);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_COLLECTION_OR_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).isEmpty();
    }

    @Test
    public void should_not_mark_dirty_on_clear_when_empty() throws Exception {

        Set<Object> target = new HashSet<>();
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.clear();

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_return_true_on_contains() throws Exception {
        SetWrapper wrapper = prepareSetWrapper(Sets.<Object>newHashSet("a", "b"));
        when(proxifier.removeProxy("a")).thenReturn("a");
        assertThat(wrapper.contains("a")).isTrue();
    }

    @Test
    public void should_return_true_on_contains_all() throws Exception {
        SetWrapper wrapper = prepareSetWrapper(Sets.<Object>newHashSet("a", "b", "c", "d"));

        List<Object> check = Arrays.<Object>asList("a", "c");
        when(proxifier.removeProxy(check)).thenReturn(check);
        assertThat(wrapper.containsAll(check)).isTrue();
    }

    @Test
    public void should_return_true_on_empty_target() throws Exception {
        SetWrapper wrapper = prepareSetWrapper(new HashSet<>());
        assertThat(wrapper.isEmpty()).isTrue();
    }

    @Test
    public void should_mark_dirty_on_remove() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a", "b");
        SetWrapper wrapper = prepareSetWrapper(target);
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.remove("a");

        assertThat(target).containsExactly("b");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a");
    }

    @Test
    public void should_not_mark_dirty_on_remove_when_no_match() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a", "b");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.remove("c");

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_remove_all() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.setProxifier(proxifier);

        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.removeAll(list);

        assertThat(target).containsExactly("b");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a", "c");
    }

    @Test
    public void should_not_mark_dirty_on_remove_all_when_no_match() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.removeAll(Arrays.asList("d", "e"));

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_retain_all() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.setProxifier(proxifier);
        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.retainAll(list);

        assertThat(target).containsOnly("a", "c");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("b");
    }

    @Test
    public void should_not_mark_dirty_on_retain_all_when_all_match() throws Exception {

        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);
        wrapper.setProxifier(proxifier);
        Collection<String> list = Arrays.asList("a", "b", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.retainAll(list);

        assertThat(target).containsOnly("a", "b", "c");

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_not_mark_dirty_on_iterator_remove() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);

        Iterator<Object> iteratorWrapper = wrapper.iterator();

        iteratorWrapper.next();
        iteratorWrapper.remove();

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_return_size() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);
        assertThat(wrapper.size()).isEqualTo(3);
    }

    @Test
    public void should_return_array() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);

        when(propertyMeta.type()).thenReturn(PropertyType.LIST);
        assertThat(wrapper.toArray()).contains("a", "b", "c");
    }

    @Test
    public void should_return_array_with_argument() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a", "b", "c");
        SetWrapper wrapper = prepareSetWrapper(target);

        when(propertyMeta.type()).thenReturn(PropertyType.LIST);
        assertThat(wrapper.toArray(new String[]{"a", "c"})).contains("a", "c");
    }

    @Test
    public void should_return_target() throws Exception {
        Set<Object> target = Sets.<Object>newHashSet("a");
        SetWrapper wrapper = new SetWrapper(target);
        assertThat(wrapper.getTarget()).isSameAs(target);
    }

    private SetWrapper prepareSetWrapper(Set<Object> target) {
        SetWrapper wrapper = new SetWrapper(target);
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);
        wrapper.setProxifier(proxifier);
        return wrapper;
    }
}
