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

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ITERATOR;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.type.ConsistencyLevel;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryExecutorTest {

    @InjectMocks
    private SliceQueryExecutor executor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext configContext;

    @Mock
    private BoundStatementWrapper bsWrapper;

    @Mock
    private EntityMapper mapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private PersistenceContextFactory contextFactory;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private PersistenceContext context;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Iterator<Row> iterator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock
    private SliceQueryProperties<ClusteredEntity> sliceQueryProperties;

    @Mock
    private ClusteredEntity entity;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet;

    @Mock
    private ListenableFuture<List<Row>> futureRows;


    @Captor
    private ArgumentCaptor<Function<List<Row>, List<ClusteredEntity>>> rowsToEntitiesCaptor;

    @Captor
    private ArgumentCaptor<Function<List<ClusteredEntity>, List<ClusteredEntity>>> isoEntitiesCaptor;

    @Mock
    private ListenableFuture<List<ClusteredEntity>> futureEntities;

    @Mock
    private AchillesFuture<List<ClusteredEntity>> achillesFutureEntities;

    @Mock
    private ListenableFuture<Iterator<Row>> futureIteratorRow;

    @Captor
    private ArgumentCaptor<Function<Iterator<Row>, Iterator<ClusteredEntity>>> rowToEntityIteratorCaptor;

    @Mock
    private ListenableFuture<Iterator<ClusteredEntity>> futureIteratorEntities;

    @Mock
    private AchillesFuture<Iterator<ClusteredEntity>> achillesFutureIteratorEntities;

    @Mock
    private ListenableFuture<Empty> futureEmpty;

    @Mock
    private AchillesFuture<Empty> achillesFutureEmpty;

    private Long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    private List<Object> partitionComponents = Arrays.<Object>asList(partitionKey);

    private ConsistencyLevel defaultReadLevel = ConsistencyLevel.EACH_QUORUM;
    private ConsistencyLevel defaultWriteLevel = ConsistencyLevel.LOCAL_QUORUM;
    private FutureCallback<Object>[] asyncListeners = new FutureCallback[] { };

    @Before
    public void setUp() {
        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(defaultReadLevel);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(defaultWriteLevel);
        when(context.getEntityFacade()).thenReturn(entityFacade);
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.forSliceQuery().getClusteringOrderForSliceQuery()).thenReturn(new ClusteringOrder("col", Sorting.ASC));

        sliceQueryProperties = SliceQueryProperties.builder(meta,ClusteredEntity.class, SliceQueryProperties.SliceType.SELECT);

        Whitebox.setInternalState(sliceQueryProperties,"asyncListeners",asyncListeners);

        executor = new SliceQueryExecutor(contextFactory,configContext,daoContext);

        executor.proxifier = proxifier;
        executor.mapper = mapper;
        executor.executorService = executorService;
        executor.asyncUtils = asyncUtils;
    }

    @Test
    public void should_get_clustered_entities_async() throws Exception {

        Row row = mock(Row.class);
        List<Row> rows = asList(row);

        when(daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel)).thenReturn(bsWrapper);

        when(daoContext.execute(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROWS)).thenReturn(futureRows);
        when(asyncUtils.transformFuture(eq(futureRows), rowsToEntitiesCaptor.capture())).thenReturn(futureEntities);
        when(asyncUtils.transformFuture(eq(futureEntities), isoEntitiesCaptor.capture())).thenReturn(futureEntities);
        when(asyncUtils.buildInterruptible(futureEntities)).thenReturn(achillesFutureEntities);

        when(meta.forOperations().instanciate()).thenReturn(entity);
        when(contextFactory.newContext(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        // When
        AchillesFuture<List<ClusteredEntity>> actual = executor.asyncGet(sliceQueryProperties);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntities);

        final Function<List<Row>, List<ClusteredEntity>> rowsToEntities = rowsToEntitiesCaptor.getValue();
        List<ClusteredEntity> entities = rowsToEntities.apply(rows);
        assertThat(entities).containsExactly(entity);
        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);

        final Function<List<ClusteredEntity>, List<ClusteredEntity>> entitiesFunction = isoEntitiesCaptor.getValue();
        entities = entitiesFunction.apply(asList(entity));
        assertThat(entities).containsExactly(entity);

        verify(asyncUtils).maybeAddAsyncListeners(futureEntities, asyncListeners);
    }

    @Test
    public void should_create_iterator_for_clustered_entities_async() throws Exception {
        when(daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel)).thenReturn(bsWrapper);

        when(daoContext.execute(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ITERATOR)).thenReturn(futureIteratorRow);
        when(asyncUtils.transformFuture(eq(futureIteratorRow), rowToEntityIteratorCaptor.capture())).thenReturn(futureIteratorEntities);
        when(asyncUtils.buildInterruptible(futureIteratorEntities)).thenReturn(achillesFutureIteratorEntities);

        when(contextFactory.newContextForSliceQuery(ClusteredEntity.class, partitionComponents, LOCAL_QUORUM)).thenReturn(context);
        final AchillesFuture<Iterator<ClusteredEntity>> actual = executor.asyncIterator(sliceQueryProperties);

        assertThat(actual).isSameAs(achillesFutureIteratorEntities);

        final Function<Iterator<Row>, Iterator<ClusteredEntity>> iteratorFunction = rowToEntityIteratorCaptor.getValue();
        final Iterator<ClusteredEntity> entitiesIterator = iteratorFunction.apply(iterator);

        assertThat(entitiesIterator).isNotNull().isInstanceOf(SliceQueryIterator.class);

        verify(asyncUtils).maybeAddAsyncListeners(futureIteratorEntities, asyncListeners);
    }

    @Test
    public void should_delete_clustered_entities() throws Exception {
        when(daoContext.bindForSliceQueryDelete(sliceQueryProperties, defaultWriteLevel)).thenReturn(bsWrapper);
        when(daoContext.execute(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFutureToEmpty(futureResultSet, executorService)).thenReturn(futureEmpty);
        when(asyncUtils.buildInterruptible(futureEmpty)).thenReturn(achillesFutureEmpty);

        final AchillesFuture<Empty> actual = executor.asyncDelete(sliceQueryProperties);

        assertThat(actual).isSameAs(achillesFutureEmpty);
        verify(daoContext).execute(bsWrapper);

    }
}
