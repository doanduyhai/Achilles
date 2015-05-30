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
package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.NOT_MANAGED;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaRowExtractor;
import info.archinnov.achilles.test.parser.entity.CompoundPK;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ColumnDefinitionBuilder;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityMapperTest {

    @InjectMocks
    private EntityMapper entityMapper;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private RowMethodInvoker cqlRowInvoker;

    @Mock
    private Row row;

    @Mock
    private ColumnDefinitions columnDefs;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta pm;

    @Captor
    private ArgumentCaptor<InternalCounterImpl> counterCaptor;

    private Definition def1;
    private Definition def2;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Test
    public void should_set_non_counter_properties_to_entity() throws Exception {

        when(pm.structure().isCompoundPK()).thenReturn(false);
        when(pm.getPropertyName()).thenReturn("name");
        when(entityMeta.getAllMetasExceptCounters()).thenReturn(asList(pm));

        when(row.isNull("name")).thenReturn(false);
        when(pm.forRowExtraction().invokeOnRowForFields(row)).thenReturn("value");

        entityMapper.setNonCounterPropertiesToEntity(row, entityMeta, entity, NOT_MANAGED);

        verify(pm.forValues()).setValueToField(entity, "value");
    }

    @Test
    public void should_set_value_to_clustered_counter_entity() throws Exception {
        //Given
        Long counterValue = 10L;
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(counterMeta.getCQLColumnName()).thenReturn("counter");
        when(entityMeta.getAllCounterMetas()).thenReturn(asList(counterMeta));
        when(cqlRowInvoker.invokeOnRowForType(row, Long.class, "counter")).thenReturn(counterValue);

        //When
        entityMapper.setValuesToClusteredCounterEntity(row, entityMeta, entity);

        //Then
        verify(counterMeta.forValues()).setValueToField(eq(entity), counterCaptor.capture());

        assertThat(counterCaptor.getValue().get()).isEqualTo(counterValue);
    }

    @Test
    public void should_set_null_to_entity_when_no_value_from_row() throws Exception {
        when(pm.structure().isCompoundPK()).thenReturn(false);
        when(pm.getPropertyName()).thenReturn("name");

        when(row.isNull("name")).thenReturn(true);

        entityMapper.setNonCounterPropertiesToEntity(row, entityMeta, entity, NOT_MANAGED);

        verify(pm.forValues(), never()).setValueToField(eq(entity), any());
        verifyZeroInteractions(cqlRowInvoker);
    }

    @Test
    public void should_do_nothing_when_null_row() throws Exception {
        entityMapper.setPropertyToEntity(null, entityMeta, pm, entity, NOT_MANAGED);

        verifyZeroInteractions(cqlRowInvoker);
    }

    @Test
    public void should_set_compound_key_to_entity() throws Exception {
        //Given
        CompoundPK compoundPK = new CompoundPK();
        PropertyMetaRowExtractor rowExtractor = mock(PropertyMetaRowExtractor.class);
        when(pm.forRowExtraction()).thenReturn(rowExtractor);
        when(pm.structure().isCompoundPK()).thenReturn(true);
        when(rowExtractor.extractCompoundPrimaryKeyFromRow(row, entityMeta, MANAGED)).thenReturn(compoundPK);

        //When
        entityMapper.setPropertyToEntity(row, entityMeta, pm, entity, MANAGED);

        //Then
        verify(pm.forValues()).setValueToField(entity, compoundPK);
    }

    @Test
    public void should_map_row_to_entity() throws Exception {
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta valueMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(idMeta.structure().isCompoundPK()).thenReturn(false);

        Map<String, PropertyMeta> propertiesMap = ImmutableMap.of("id", idMeta, "value", valueMeta);

        def1 = ColumnDefinitionBuilder.buildColumnDef("keyspace", "table", "id", DataType.bigint());
        def2 = ColumnDefinitionBuilder.buildColumnDef("keyspace", "table", "value", DataType.text());

        when(row.getColumnDefinitions()).thenReturn(columnDefs);
        when(columnDefs.iterator()).thenReturn(asList(def1, def2).iterator());

        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.forOperations().instanciate()).thenReturn(entity);
        when(idMeta.forRowExtraction().invokeOnRowForFields(row)).thenReturn(id);
        when(valueMeta.forRowExtraction().invokeOnRowForFields(row)).thenReturn("value");
        when(entityMeta.forOperations().instanciate()).thenReturn(entity);

        CompleteBean actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, propertiesMap, MANAGED);

        assertThat(actual).isSameAs(entity);
        verify(idMeta.forValues()).setValueToField(entity, id);
        verify(valueMeta.forValues()).setValueToField(entity, "value");
    }

    @Test
    public void should_map_row_to_entity_with_primary_key() throws Exception {
        ClusteredEntity entity = new ClusteredEntity();
        CompoundPK compoundPK = new CompoundPK();
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(idMeta.structure().isCompoundPK()).thenReturn(true);

        Map<String, PropertyMeta> propertiesMap = new HashMap<>();

        when(row.getColumnDefinitions()).thenReturn(columnDefs);
        when(columnDefs.iterator()).thenReturn(Arrays.<Definition>asList().iterator());
        when(entityMeta.forOperations().instanciate()).thenReturn(entity);
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(idMeta.forRowExtraction().extractCompoundPrimaryKeyFromRow(row, entityMeta, MANAGED)).thenReturn(compoundPK);

        ClusteredEntity actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, propertiesMap, MANAGED);

        assertThat(actual).isSameAs(entity);
        verify(idMeta.forValues()).setValueToField(entity, compoundPK);
    }

    @Test
    public void should_not_map_row_to_entity_with_primary_key_when_entity_null() {
        ClusteredEntity actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, null, MANAGED);

        assertThat(actual).isNull();
    }

    @Test
    public void should_return_null_when_no_column_found() throws Exception {
        when(row.getColumnDefinitions()).thenReturn(null);
        when(entityMeta.forOperations().instanciate()).thenReturn(entity);

        CompleteBean actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, null, MANAGED);
        assertThat(actual).isNull();
    }

}
