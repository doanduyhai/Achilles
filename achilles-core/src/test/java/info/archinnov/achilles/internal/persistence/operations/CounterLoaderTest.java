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

import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

@RunWith(MockitoJUnitRunner.class)
public class CounterLoaderTest {

    @InjectMocks
    private CounterLoader loader;

    @Mock
    private EntityMapper mapper;

    @Mock
    private ConsistencyOverrider overrider;

    @Mock
    private PersistenceContext.EntityFacade context;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private PropertyMeta counterMeta;

    private Object primaryKey = RandomUtils.nextLong();

    private Object entity = new Object();

    @Test
    public void should_load_clustered_counters() throws Exception {
        // Given
        Row row = mock(Row.class);

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getReadLevel(context)).thenReturn(ONE);
        when(context.getClusteredCounter()).thenReturn(row);

        when(meta.instanciate()).thenReturn(entity);
        when(meta.getIdMeta()).thenReturn(idMeta);

        when(context.getAllCountersMeta()).thenReturn(Arrays.asList(counterMeta));

        // When

        Object actual = loader.loadClusteredCounters(context);

        // Then
        assertThat(actual).isSameAs(entity);

        verify(mapper).setCounterToEntity(counterMeta, entity, row);
    }

    @Test
    public void should_not_load_clustered_counters_when_not_found() throws Exception {
        // Given

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getReadLevel(context)).thenReturn(ONE);

        // When

        Object actual = loader.loadClusteredCounters(context);

        // Then
        assertThat(actual).isNull();

        verifyZeroInteractions(mapper);
    }

    @Test
    public void should_load_clustered_counter_column() throws Exception {
        // Given
        final long counterValue = 11L;
        when(overrider.getReadLevel(context, counterMeta)).thenReturn(ONE);
        when(context.getClusteredCounterColumn(counterMeta)).thenReturn(counterValue);

        // When
        loader.loadClusteredCounterColumn(context, entity, counterMeta);

        // Then
        verify(mapper).setCounterToEntity(counterMeta, entity, counterValue);
    }

    @Test
    public void should_load_counter() throws Exception {
        // Given
        final long counterValue = 11L;
        when(overrider.getReadLevel(context, counterMeta)).thenReturn(ONE);
        when(context.getSimpleCounter(counterMeta, ONE)).thenReturn(counterValue);

        // When

        loader.loadCounter(context, entity, counterMeta);

        // Then
        verify(mapper).setCounterToEntity(counterMeta, entity, counterValue);
    }
}
