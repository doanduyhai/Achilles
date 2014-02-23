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

package info.archinnov.achilles.internal.proxy.dirtycheck;

import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet.ElementAtIndex;
import static org.fest.assertions.api.Assertions.*;
import static org.fest.assertions.data.MapEntry.entry;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class DirtyCheckChangeSetTest {

    @Mock
    private PropertyMeta pm;

    private DirtyCheckChangeSet changeSet;

    private Update.Assignments with;

    @Before
    public void setUp() {
        changeSet  = new DirtyCheckChangeSet(pm, ADD_TO_MAP);
        when(pm.getPropertyName()).thenReturn("property");
    }

    @Test
    public void should_get_encoded_list_changes() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.encode(changeSet.listChanges)).thenReturn(changeSet.listChanges);

        //When
        List<Object> actual = changeSet.getEncodedListChanges();

        //Then
        assertThat(actual).containsExactly("a");
    }

    @Test
    public void should_get_raw_list_changes() throws Exception {
        //When
        List<Object> actual = changeSet.getEncodedListChanges();

        //Then
        assertThat(actual).isEmpty();
        verifyZeroInteractions(pm);
    }

    @Test
    public void should_get_encoded_list_change_at_index() throws Exception {
        //Given
        changeSet.listChangeAtIndex = new ElementAtIndex(0,"a");
        when(pm.encode("a")).thenReturn("a");

        //When
        ElementAtIndex actual = changeSet.getEncodedListChangeAtIndex();

        //Then
        assertThat(actual).isEqualTo(changeSet.listChangeAtIndex);
    }

    @Test
    public void should_get_null_list_change_at_index() throws Exception {
        //When
        ElementAtIndex actual = changeSet.getEncodedListChangeAtIndex();

        //Then
        assertThat(actual).isNull();
        verifyZeroInteractions(pm);
    }

    @Test
    public void should_get_encoded_set_changes() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        when(pm.encode(changeSet.setChanges)).thenReturn(changeSet.setChanges);

        //When
        Set<Object> actual = changeSet.getEncodedSetChanges();

        //Then
        assertThat(actual).containsExactly("a");
    }

    public void should_get_raw_set_changes() throws Exception {
        //When
        Set<Object> actual = changeSet.getEncodedSetChanges();

        //Then
        assertThat(actual).isEmpty();
        verifyZeroInteractions(pm);
    }

    @Test
    public void should_get_encoded_map_changes() throws Exception {
        //Given
        changeSet.mapChanges.put(1,"a");
        when(pm.encodeKey(1)).thenReturn(1);
        when(pm.encode("a")).thenReturn("a");

        //When
        Map<Object,Object> actual = changeSet.getEncodedMapChanges();

        //Then
        assertThat(actual).contains(entry(1, "a"));
    }

    public void should_get_raw_map_changes() throws Exception {
        //When
        Map<Object,Object> actual = changeSet.getEncodedMapChanges();

        //Then
        assertThat(actual).isEmpty();
        verifyZeroInteractions(pm);
    }

    @Test
    public void should_generate_update_for_added_elements() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        when(pm.encode(changeSet.setChanges)).thenReturn(changeSet.setChanges);

        //When
        Object[] vals = changeSet.generateUpdateForAddedElements(update(), false);

        //Then
        assertThat((Set<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_added_elements_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForAddedElements(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_removed_elements() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        when(pm.encode(changeSet.setChanges)).thenReturn(changeSet.setChanges);

        //When
        Object[] vals = changeSet.generateUpdateForRemovedElements(update(), false);

        //Then
        assertThat((Set<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property-?;");
    }

    @Test
    public void should_generate_update_for_removed_element_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForRemovedElements(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property-:property;");
    }


    @Test
    public void should_generate_update_for_appended_elements() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.encode(changeSet.listChanges)).thenReturn(changeSet.listChanges);

        //When
        Object[] vals = changeSet.generateUpdateForAppendedElements(update(), false);

        //Then
        assertThat((List<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_appended_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForAppendedElements(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_prepended_elements() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.encode(changeSet.listChanges)).thenReturn(changeSet.listChanges);

        //When
        Object[] vals = changeSet.generateUpdateForPrependedElements(update(), false);

        //Then
        assertThat((List<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=?+property;");
    }

    @Test
    public void should_generate_update_for_prepended_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForPrependedElements(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=:property+property;");
    }

    @Test
    public void should_generate_update_for_remove_list_element() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.encode(changeSet.listChanges)).thenReturn(changeSet.listChanges);

        //When
        Object[] vals = changeSet.generateUpdateForRemoveListElements(update(), false);

        //Then
        assertThat((List<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property-?;");
    }

    @Test
    public void should_generate_update_for_remove_list_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForRemoveListElements(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property-:property;");
    }

    @Test
    public void should_generate_update_for_set_element_at_index() throws Exception {
        //Given
        changeSet.listChangeAtIndex = new ElementAtIndex(1,"a");
        when(pm.encode("a")).thenReturn("a");

        //When
        Object[] vals = changeSet.generateUpdateForSetAtIndexElement(update());

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isEqualTo("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property[1]=?;");
    }

    @Test
    public void should_generate_update_for_remove_element_at_index() throws Exception {
        //Given
        changeSet.listChangeAtIndex = new ElementAtIndex(1,"a");
        when(pm.encode("a")).thenReturn("a");

        //When
        Object[] vals = changeSet.generateUpdateForRemovedAtIndexElement(update());

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property[1]=null;");
    }

    @Test
    public void should_generate_update_for_added_entries() throws Exception {
        //Given
        changeSet.mapChanges.put(1,"a");
        when(pm.encodeKey(1)).thenReturn(1);
        when(pm.encode("a")).thenReturn("a");

        //When
        Object[] vals = changeSet.generateUpdateForAddedEntries(update(), false);

        //Then
        assertThat((Map<Object,Object>)vals[0]).contains(entry(1, "a"));
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_added_entries_with_bind_marker() throws Exception {
        //Given
        changeSet.mapChanges.put(1,"a");
        when(pm.encodeKey(1)).thenReturn(1);
        when(pm.encode("a")).thenReturn("a");

        //When
        Object[] vals = changeSet.generateUpdateForAddedEntries(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_removed_key() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        when(pm.encodeKey(1)).thenReturn(1);

        //When
        Object[] vals = changeSet.generateUpdateForRemovedKey(update(), false);

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property[1]=null;");
    }

    @Test
    public void should_generate_update_for_removed_key_with_bind_marker() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        when(pm.encodeKey(1)).thenReturn(1);

        //When
        Object[] vals = changeSet.generateUpdateForRemovedKey(update(), true);

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property[:key]=:nullValue;");
    }


    @Test
    public void should_generate_update_for_remove_all() throws Exception {
        //When
        Object[] vals = changeSet.generateUpdateForRemoveAll(update(), false);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=null;");
    }

    @Test
    public void should_generate_update_for_remove_all_with_bind_marker() throws Exception {
        //When
        Object[] vals = changeSet.generateUpdateForRemoveAll(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=:property;");
    }

    @Test
    public void should_generate_update_for_assign_list_value() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.encode(changeSet.listChanges)).thenReturn(changeSet.listChanges);

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToList(update(), false);

        //Then
        assertThat((List<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=?;");
    }

    @Test
    public void should_generate_update_for_assign_list_value_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToList(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=:property;");
    }

    @Test
    public void should_generate_update_for_assign_set_value() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        when(pm.encode(changeSet.setChanges)).thenReturn(changeSet.setChanges);

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToSet(update(), false);

        //Then
        assertThat((Set<Object>)vals[0]).containsExactly("a");
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=?;");
    }

    @Test
    public void should_generate_update_for_assign_set_value_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToSet(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=:property;");
    }

    @Test
    public void should_generate_update_for_assign_map_value() throws Exception {
        //Given
        changeSet.mapChanges.put(1,"a");
        when(pm.encodeKey(1)).thenReturn(1);
        when(pm.encode("a")).thenReturn("a");

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToMap(update(), false);

        //Then
        assertThat((Map<Object,Object>)vals[0]).contains(entry(1,"a"));
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=?;");
    }

    @Test
    public void should_generate_update_for_assign_map_value_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");

        //When
        Object[] vals = changeSet.generateUpdateForAssignValueToMap(update(), true);

        //Then
        assertThat(vals[0]).isNull();
        assertThat(with.getQueryString()).isEqualTo("UPDATE table SET property=:property;");
    }




    private Update.Assignments update() {
        with = QueryBuilder.update("table").with();
        return with;
    }
}
