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
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest {

    @InjectMocks
    private EntityLoader loader;

    @Mock
    private EntityMapper mapper;

    @Mock
    private CounterLoader counterLoader;

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

    @Mock(answer = RETURNS_DEEP_STUBS)
    private PropertyMeta pm;

    @Mock
    private ListenableFuture<Row> futureRow;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    @Captor
    private ArgumentCaptor<Function<Row, CompleteBean>> rowToEntityCaptor;

    private Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

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
    public void should_create_empty_entity() throws Exception {
        when(meta.forOperations().instanciate()).thenReturn(entity);

        CompleteBean actual = loader.createEmptyEntity(context, CompleteBean.class);

        assertThat(actual).isSameAs(entity);

        verify(idMeta.forValues()).setValueToField(actual, primaryKey);
    }

    @Test
    public void should_load_simple_entity() throws Exception {
        // Given
        Row row = mock(Row.class);
        when(meta.structure().isClusteredCounter()).thenReturn(false);
        when(context.loadEntity()).thenReturn(futureRow);
        when(meta.forOperations().instanciate()).thenReturn(entity);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(row);
        assertThat(actualEntity).isSameAs(entity);

        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);

        verifyZeroInteractions(counterLoader);
    }

    @Test
    public void should_not_load_simple_entity_when_not_found() throws Exception {
        // Given
        when(meta.structure().isClusteredCounter()).thenReturn(false);
        when(context.loadEntity()).thenReturn(futureRow);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(null);
        assertThat(actualEntity).isNull();
        verify(meta.forOperations(), never()).instanciate();
        verifyZeroInteractions(counterLoader, mapper);
    }

    @Test
    public void should_load_clustered_counter_entity() throws Exception {
        // Given
        when(meta.structure().isClusteredCounter()).thenReturn(true);
        when(counterLoader.<CompleteBean>loadClusteredCounters(context)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        verifyZeroInteractions(asyncUtils, mapper);
    }

    @Test
    public void should_load_properties_into_object() throws Exception {
        // Given
        when(pm.structure().isCounter()).thenReturn(false);
        Row row = mock(Row.class);
        when(context.loadProperty(pm)).thenReturn(row);

        // When
        loader.loadPropertyIntoObject(context, entity, pm);

        // Then
        verify(mapper).setPropertyToEntity(row, meta, pm, entity);
        verifyZeroInteractions(counterLoader);
    }

    @Test
    public void should_switch_null_with_NullRow_for_collection_and_map() throws Exception {
        // Given
        ArgumentCaptor<Row> rowCaptor = ArgumentCaptor.forClass(Row.class);
        when(pm.structure().isCounter()).thenReturn(false);
        when(pm.structure().isCollectionAndMap()).thenReturn(true);

        when(context.loadProperty(pm)).thenReturn(null);

        // When
        loader.loadPropertyIntoObject(context, entity, pm);

        // Then
        verify(mapper).setPropertyToEntity(rowCaptor.capture(), eq(meta), eq(pm), eq(entity));
        assertThat(rowCaptor.getValue()).isInstanceOf(NullRow.class);

        verifyZeroInteractions(counterLoader);
    }

    @Test
    public void should_load_counter_properties_into_object() throws Exception {
        // Given
        when(pm.structure().isCounter()).thenReturn(true);

        // When
        loader.loadPropertyIntoObject(context, entity, pm);

        // Then
        verify(counterLoader).loadCounter(context, entity, pm);
        verifyZeroInteractions(mapper);
    }
}
