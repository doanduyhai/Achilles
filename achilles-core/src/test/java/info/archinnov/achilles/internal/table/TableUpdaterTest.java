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
package info.archinnov.achilles.internal.table;

import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.valueClass;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.ColumnMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.type.Counter;

@RunWith(MockitoJUnitRunner.class)
public class TableUpdaterTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private TableUpdater updater;

    @Mock
    private Session session;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    @Mock
    private KeyspaceMetadata keyspaceMeta;

    @Mock
    private TableMetadata tableMeta;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    private String keyspaceName = "achilles";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Before
    public void setUp() throws Exception {
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTables()).thenReturn(new ArrayList<TableMetadata>());
        when(keyspaceMeta.getTables()).thenReturn(asList(tableMeta));
        when(tableMeta.getName()).thenReturn("tableName");
        when(meta.config().getQualifiedTableName()).thenReturn("tableName");
        when(meta.config().isSchemaUpdateEnabled()).thenReturn(true);

    }

    @Test
    public void should_not_update_if_schema_update_disabled() throws Exception {
        //Given
        when(meta.config().isSchemaUpdateEnabled()).thenReturn(false);

        //When
        updater.updateTableForEntity(session, meta, tableMeta);

        //Then
        verifyZeroInteractions(session);
    }

    @Test
    public void should_update_table_with_new_simple_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta longColPM = valueClass(Long.class).type(SIMPLE).cqlColumnName("longcol").staticColumn().build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(longColPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session).execute(stringCaptor.capture());
        assertThat(stringCaptor.getValue()).isEqualTo("\n\tALTER TABLE tableName ADD longcol bigint static");
    }

    @Test
    public void should_update_table_with_new_indexed_simple_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta longColPM = valueClass(Long.class).type(SIMPLE).cqlColumnName("longcol").build();
        longColPM.setIndexProperties(new IndexProperties("long_index", "longCol"));

        when(meta.getAllMetasExceptId()).thenReturn(asList(longColPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());


        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session, Mockito.times(2)).execute(stringCaptor.capture());
        final List<String> updates = stringCaptor.getAllValues();
        assertThat(updates.get(0)).isEqualTo("\n\tALTER TABLE tableName ADD longcol bigint");
        assertThat(updates.get(1)).isEqualTo("\n\tCREATE INDEX long_index ON tableName(longcol)");
    }

    @Test
    public void should_update_table_with_new_list_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta listStringPM = valueClass(String.class).type(LIST).cqlColumnName("list_string").build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(listStringPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session).execute(stringCaptor.capture());
        assertThat(stringCaptor.getValue()).isEqualTo("\n\tALTER TABLE tableName ADD list_string list<text>");
    }

    @Test
    public void should_update_table_with_new_set_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta setStringPM = valueClass(String.class).type(SET).cqlColumnName("set_string").build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(setStringPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session).execute(stringCaptor.capture());
        assertThat(stringCaptor.getValue()).isEqualTo("\n\tALTER TABLE tableName ADD set_string set<text>");
    }

    @Test
    public void should_update_table_with_new_map_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta mapStringPM = completeBean(Integer.class, String.class).type(MAP).cqlColumnName("preferences").build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(mapStringPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session).execute(stringCaptor.capture());
        assertThat(stringCaptor.getValue()).isEqualTo("\n\tALTER TABLE tableName ADD preferences map<int, text>");
    }

    @Test
    public void should_update_table_with_new_clustered_counter_field() throws Exception {
        // Given
        PropertyMeta idMeta = valueClass(Long.class).type(ID).cqlColumnName("id").build();
        PropertyMeta counterPM = valueClass(Counter.class).type(COUNTER).cqlColumnName("count").build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(counterPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());
        when(meta.structure().isClusteredCounter()).thenReturn(true);

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verify(session).execute(stringCaptor.capture());
        assertThat(stringCaptor.getValue()).isEqualTo("\n\tALTER TABLE tableName ADD count counter");
    }

    @Test
    public void should_not_add_counter_field_if_non_clustered_counter_entity() throws Exception {
        PropertyMeta idMeta = valueClass(Long.class).type(ID).propertyName("id").build();
        PropertyMeta counterPM = valueClass(Counter.class).type(COUNTER).propertyName("count").build();

        when(meta.getAllMetasExceptId()).thenReturn(asList(counterPM));
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(tableMeta.getColumns()).thenReturn(Arrays.<ColumnMetadata>asList());
        when(meta.structure().isClusteredCounter()).thenReturn(false);

        // When
        updater.updateTableForEntity(session, meta, tableMeta);

        // Then
        verifyZeroInteractions(session);
    }

}
