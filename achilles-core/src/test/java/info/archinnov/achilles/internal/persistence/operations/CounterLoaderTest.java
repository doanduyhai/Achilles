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
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class CounterLoaderTest {

    @InjectMocks
    private CounterLoader loader;

    @Mock
    private EntityMapper mapper;

    @Mock
    private ConsistencyOverrider overrider;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;


    @Mock
    private PersistenceContext.EntityFacade context;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock
    private PropertyMeta counterMeta;

    @Mock
    private ListenableFuture<Row> futureRow;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    @Captor
    private ArgumentCaptor<Function<Row, CompleteBean>> rowToEntityCaptor;

    private Object primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    private CompleteBean entity = new CompleteBean();

    @Before
    public void setUp() throws Exception {
        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.getExecutorService()).thenReturn(executorService);
        when(meta.getIdMeta()).thenReturn(idMeta);
    }

    @Test
    public void should_load_clustered_counters() throws Exception {
        // Given
        Row row = mock(Row.class);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        when(overrider.getReadLevel(context)).thenReturn(ONE);
        when(context.getClusteredCounter()).thenReturn(futureRow);
        when(meta.forOperations().instanciate()).thenReturn(entity);
        when(context.getAllCountersMeta()).thenReturn(asList(counterMeta));

        // When
        AchillesFuture<CompleteBean> actual = loader.loadClusteredCounters(context);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);
        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(row);
        assertThat(actualEntity).isSameAs(entity);

        verify(idMeta.forValues()).setValueToField(entity, primaryKey);
        verify(mapper).setCounterToEntity(counterMeta, entity, row);
    }

    @Test
    public void should_not_load_clustered_counters_when_not_found() throws Exception {
        // Given
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        when(overrider.getReadLevel(context)).thenReturn(ONE);
        when(context.getClusteredCounter()).thenReturn(futureRow);
        when(context.getAllCountersMeta()).thenReturn(asList(counterMeta));

        // When
        AchillesFuture<CompleteBean> actual = loader.loadClusteredCounters(context);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);
        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(null);
        assertThat(actualEntity).isNull();

        verify(meta.forOperations(), never()).instanciate();
        verifyZeroInteractions(idMeta, mapper);
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
