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

import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.appendAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.discardAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.prependAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.setIdx;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet.ElementAtIndex;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Update.Conditions;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

@RunWith(MockitoJUnitRunner.class)
public class DirtyCheckChangeSetTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta pm;

    private DirtyCheckChangeSet changeSet;

    private Update.Conditions conditions;

    @Before
    public void setUp() {
        changeSet = new DirtyCheckChangeSet(pm, ADD_TO_MAP);
        when(pm.getPropertyName()).thenReturn("property");
    }

    @Test
    public void should_get_encoded_list_changes() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        when(pm.forTranscoding().encodeToCassandra(changeSet.listChanges)).thenReturn(changeSet.listChanges);

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
        changeSet.listChangeAtIndex = new ElementAtIndex(0, "a");
        when(pm.forTranscoding().<List<Object>>encodeToCassandra(Arrays.asList("a"))).thenReturn(Arrays.<Object>asList("a"));

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
        when(pm.forTranscoding().encodeToCassandra(changeSet.setChanges)).thenReturn(changeSet.setChanges);

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
        changeSet.mapChanges.put(1, "a");
        when(pm.forTranscoding().encodeToCassandra(changeSet.mapChanges)).thenReturn(ImmutableMap.<Object, Object>of(1, "a"));

        //When
        Map<Object, Object> actual = changeSet.getEncodedMapChanges();

        //Then
        assertThat(actual).contains(entry(1, "a"));
    }

    public void should_get_raw_map_changes() throws Exception {
        //When
        Map<Object, Object> actual = changeSet.getEncodedMapChanges();

        //Then
        assertThat(actual).isEmpty();
        verifyZeroInteractions(pm);
    }

    @Test
    public void should_generate_update_for_added_elements() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        final Conditions update = update();

        when(pm.forStatementGeneration().generateUpdateForAddedElements(update)).thenReturn(update.with(addAll("property", bindMarker())));

        //When
        changeSet.generateUpdateForAddedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_added_elements_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForAddedElements(update)).thenReturn(update.with(addAll("property", bindMarker("property"))));


        //When
        changeSet.generateUpdateForAddedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_removed_elements() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemovedElements(update)).thenReturn(update.with(removeAll("property", bindMarker())));


        //When
        changeSet.generateUpdateForRemovedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property-?;");
    }

    @Test
    public void should_generate_update_for_removed_element_with_bind_marker() throws Exception {
        //Given
        changeSet.setChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemovedElements(update)).thenReturn(update.with(removeAll("property", bindMarker("property"))));

        //When
        changeSet.generateUpdateForRemovedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property-:property;");
    }


    @Test
    public void should_generate_update_for_appended_elements() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForAppendedElements(update)).thenReturn(update.with(appendAll("property", bindMarker())));

        //When
        changeSet.generateUpdateForAppendedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_appended_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForAppendedElements(update)).thenReturn(update.with(appendAll("property", bindMarker("property"))));

        //When
        changeSet.generateUpdateForAppendedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_prepended_elements() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForPrependedElements(update)).thenReturn(update.with(prependAll("property", bindMarker())));

        //When
        changeSet.generateUpdateForPrependedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=?+property;");
    }

    @Test
    public void should_generate_update_for_prepended_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForPrependedElements(update)).thenReturn(update.with(prependAll("property", bindMarker("property"))));

        //When
        changeSet.generateUpdateForPrependedElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=:property+property;");
    }

    @Test
    public void should_generate_update_for_remove_list_element() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemoveListElements(update)).thenReturn(update.with(discardAll("property", bindMarker())));

        //When
        changeSet.generateUpdateForRemoveListElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property-?;");
    }

    @Test
    public void should_generate_update_for_remove_list_element_with_bind_marker() throws Exception {
        //Given
        changeSet.listChanges.add("a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemoveListElements(update)).thenReturn(update.with(discardAll("property", bindMarker("property"))));

        //When
        changeSet.generateUpdateForRemoveListElements(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property-:property;");
    }

    @Test
    public void should_generate_update_for_set_element_at_index() throws Exception {
        //Given
        changeSet.listChangeAtIndex = new ElementAtIndex(1, "a");
        final Conditions update = update();
        when(pm.forTranscoding().<List<Object>>encodeToCassandra(Arrays.asList("a"))).thenReturn(Arrays.<Object>asList("a"));
        when(pm.forStatementGeneration().generateUpdateForSetAtIndexElement(update, 1, "a")).thenReturn(update.with(setIdx("property", 1, bindMarker())));

        //When
        Object[] vals = changeSet.generateUpdateForSetAtIndexElement(update).right;

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isEqualTo("a");
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property[1]=?;");
    }

    @Test
    public void should_generate_update_for_remove_element_at_index() throws Exception {
        //Given
        changeSet.listChangeAtIndex = new ElementAtIndex(1, "a");
        final Conditions update = update();
        when(pm.forTranscoding().<List<Object>>encodeToCassandra(Arrays.asList("a"))).thenReturn(Arrays.<Object>asList("a"));
        when(pm.forStatementGeneration().generateUpdateForSetAtIndexElement(update, 1, "a")).thenReturn(update.with(setIdx("property", 1, null)));

        //When
        Object[] vals = changeSet.generateUpdateForRemovedAtIndexElement(update).right;

        //Then
        assertThat(vals[0]).isEqualTo(1);
        assertThat(vals[1]).isNull();
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property[1]=null;");
    }

    @Test
    public void should_generate_update_for_added_entries() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForAddedEntries(update)).thenReturn(update.with(putAll("property", bindMarker())));

        //When
        changeSet.generateUpdateForAddedEntries(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+?;");
    }

    @Test
    public void should_generate_update_for_added_entries_with_bind_marker() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForAddedEntries(update)).thenReturn(update.with(putAll("property", bindMarker("property"))));

        //When
        changeSet.generateUpdateForAddedEntries(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=property+:property;");
    }

    @Test
    public void should_generate_update_for_removed_key() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemovedKey(update)).thenReturn(update.with(put("property", 1, null)));

        //When
        changeSet.generateUpdateForRemovedKey(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property[1]=null;");
    }

    @Test
    public void should_generate_update_for_removed_key_with_bind_marker() throws Exception {
        //Given
        changeSet.mapChanges.put(1, "a");
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemovedKey(update)).thenReturn(update.with(put("property", bindMarker("key"), bindMarker("nullValue"))));

        //When
        changeSet.generateUpdateForRemovedKey(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property[:key]=:nullValue;");
    }


    @Test
    public void should_generate_update_for_remove_all() throws Exception {
        //When
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemoveAll(update)).thenReturn(update.with(set("property", null)));

        changeSet.generateUpdateForRemoveAll(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=null;");
    }

    @Test
    public void should_generate_update_for_remove_all_with_bind_marker() throws Exception {
        //When
        final Conditions update = update();
        when(pm.forStatementGeneration().generateUpdateForRemoveAll(update)).thenReturn(update.with(set("property", bindMarker("property"))));

        changeSet.generateUpdateForRemoveAll(update);

        //Then
        assertThat(conditions.getQueryString()).isEqualTo("UPDATE table SET property=:property;");
    }

    private Update.Conditions update() {
        conditions = QueryBuilder.update("table").onlyIf();
        return conditions;
    }
}
