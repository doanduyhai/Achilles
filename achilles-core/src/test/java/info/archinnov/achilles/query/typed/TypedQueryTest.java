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
package info.archinnov.achilles.query.typed;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ITERATOR;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROW;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.PARTITION_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.iterator.AchillesIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class TypedQueryTest {

    private TypedQuery<CompleteBean> typedQuery;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private EntityMapper mapper;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private PersistenceContextFactory contextFactory;

    @Mock
    private PersistenceContext context;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private Row row;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet;

    @Mock
    private ListenableFuture<List<Row>> futureRows;

    @Mock
    private ListenableFuture<Row> futureRow;

    @Mock
    private ListenableFuture<List<CompleteBean>> futureEntities;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private ListenableFuture<Iterator<Row>> futureIteratorRow;

    @Mock
    private ListenableFuture<Iterator<CompleteBean>> futureIteratorEntity;

    @Mock
    private AchillesFuture<List<CompleteBean>> achillesFuturesEntities;

    @Mock
    private AchillesFuture<CompleteBean> achillesFuturesEntity;

    @Mock
    private AchillesFuture<Iterator<CompleteBean>> achillesFuturesIteratorEntity;



    private FutureCallback<Object>[] asyncListeners = new FutureCallback[] { };

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Captor
    private ArgumentCaptor<Function<List<Row>, List<CompleteBean>>> rowsToEntitiesCaptor;

    @Captor
    private ArgumentCaptor<Function<Row, CompleteBean>> rowToEntityCaptor;

    @Captor
    private ArgumentCaptor<Function<List<CompleteBean>, List<CompleteBean>>> isoEntitiesCaptor;

    @Captor
    private ArgumentCaptor<Function<CompleteBean, CompleteBean>> isoEntityCaptor;

    @Captor
    private ArgumentCaptor<Function<Iterator<Row>, Iterator<CompleteBean>>> iteratorCaptor;

    private Class<CompleteBean> entityClass = CompleteBean.class;

    private CompleteBean entity = new CompleteBean();

    @Before
    public void setUp() {
        when(context.getEntityFacade()).thenReturn(entityFacade);
    }

    @Test
    public void should_get_all_managed_with_select_star_async() throws Exception {
        // Given
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class)
                .propertyName("id")
                .type(PARTITION_KEY)
                .cqlColumnName("id")
                .accessors().build();

        PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class)
                .propertyName("name")
                .type(SIMPLE)
                .cqlColumnName("name")
                .accessors().build();

        EntityMeta meta = buildEntityMeta(idMeta, nameMeta);

        RegularStatement statement = select().from("test");

        initTypedQuery(statement, meta, meta.getPropertyMetas(), MANAGED);

        when(daoContext.execute(any(AbstractStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROWS)).thenReturn(futureRows);
        when(asyncUtils.transformFuture(eq(futureRows), rowsToEntitiesCaptor.capture())).thenReturn(futureEntities);
        when(asyncUtils.transformFuture(eq(futureEntities), isoEntitiesCaptor.capture())).thenReturn(futureEntities);
        when(asyncUtils.buildInterruptible(futureEntities)).thenReturn(achillesFuturesEntities);

        when(mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>>any(), eq(MANAGED))).thenReturn(entity);
        when(contextFactory.newContext(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        // When
        final AchillesFuture<List<CompleteBean>> actual = typedQuery.asyncGetInternal(asyncListeners);

        // Then
        assertThat(actual).isSameAs(achillesFuturesEntities);
        verify(asyncUtils).maybeAddAsyncListeners(futureEntities, asyncListeners);

        final Function<List<Row>, List<CompleteBean>> rowsToEntities = rowsToEntitiesCaptor.getValue();
        final List<CompleteBean> entities = rowsToEntities.apply(asList(row));
        assertThat(entities).containsExactly(entity);

        final List<Function<List<CompleteBean>, List<CompleteBean>>> entitiesFunctions = isoEntitiesCaptor.getAllValues();

        final List<CompleteBean> entitiesWithTriggers = entitiesFunctions.get(0).apply(asList(entity));
        assertThat(entitiesWithTriggers).containsExactly(entity);
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);

        final List<CompleteBean> entitiesWithProxy = entitiesFunctions.get(1).apply(asList(entity));
        assertThat(entitiesWithProxy).containsExactly(entity);
    }


    @Test
    public void should_get_first_entity_async() throws Exception {
        // When
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.PARTITION_KEY).cqlColumnName("id").accessors().build();

        PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).propertyName("name")
                .type(PropertyType.SIMPLE).cqlColumnName("name").accessors().build();

        EntityMeta meta = buildEntityMeta(idMeta, nameMeta);

        RegularStatement statement = select("id").from("test");
        initTypedQuery(statement, meta, meta.getPropertyMetas(), MANAGED);

        when(daoContext.execute(any(AbstractStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.transformFuture(eq(futureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFuturesEntity);

        when(mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>>any(), eq(MANAGED))).thenReturn(entity);
        when(contextFactory.newContext(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        // When
        final AchillesFuture<CompleteBean> actual = typedQuery.asyncGetFirstInternal(asyncListeners);

        // Then
        assertThat(actual).isSameAs(achillesFuturesEntity);
        verify(asyncUtils).maybeAddAsyncListeners(futureEntity, asyncListeners);

        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(row);
        assertThat(actualEntity).isSameAs(entity);

        final List<Function<CompleteBean, CompleteBean>> captured = isoEntityCaptor.getAllValues();
        final CompleteBean applyTriggers = captured.get(0).apply(entity);
        assertThat(applyTriggers).isSameAs(entity);
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);

        final CompleteBean proxifiedEntity = captured.get(1).apply(entity);
        assertThat(proxifiedEntity).isSameAs(entity);
    }

    @Test
    public void should_get_iterator_without_fetch_size() throws Exception {
        //Given
        RegularStatement statement = select("id").from("test");
        initTypedQuery(statement, meta, meta.getPropertyMetas(), MANAGED);

        when(contextFactory.newContextForTypedQuery(entityClass)).thenReturn(context);
        when(daoContext.execute(any(AbstractStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ITERATOR, executorService)).thenReturn(futureIteratorRow);
        when(asyncUtils.transformFuture(eq(futureIteratorRow), iteratorCaptor.capture())).thenReturn(futureIteratorEntity);

        when(asyncUtils.buildInterruptible(futureIteratorEntity)).thenReturn(achillesFuturesIteratorEntity);
        when(achillesFuturesIteratorEntity.getImmediately()).thenReturn(Arrays.asList(entity).iterator());

        //When
        final Iterator<CompleteBean> iterator = typedQuery.iterator();

        //Then
        assertThat(iterator.next()).isSameAs(entity);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iteratorCaptor.getValue().apply(Arrays.asList(row).iterator())).isInstanceOf(AchillesIterator.class);
        verify(asyncUtils).maybeAddAsyncListeners(futureIteratorEntity, asyncListeners);
    }

    @Test
    public void should_get_iterator_with_fetch_size() throws Exception {
        //Given
        RegularStatement statement = select("id").from("test");
        initTypedQuery(statement, meta, meta.getPropertyMetas(), MANAGED);

        when(contextFactory.newContextForTypedQuery(entityClass)).thenReturn(context);
        when(daoContext.execute(any(AbstractStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ITERATOR, executorService)).thenReturn(futureIteratorRow);
        when(asyncUtils.transformFuture(eq(futureIteratorRow), iteratorCaptor.capture())).thenReturn(futureIteratorEntity);

        when(asyncUtils.buildInterruptible(futureIteratorEntity)).thenReturn(achillesFuturesIteratorEntity);
        when(achillesFuturesIteratorEntity.getImmediately()).thenReturn(Arrays.asList(entity).iterator());

        //When
        final Iterator<CompleteBean> iterator = typedQuery.iterator(123);

        //Then
        assertThat(typedQuery.nativeStatementWrapper.getStatement().getFetchSize()).isEqualTo(123);
        assertThat(iterator.next()).isSameAs(entity);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iteratorCaptor.getValue().apply(Arrays.asList(row).iterator())).isInstanceOf(AchillesIterator.class);
        verify(asyncUtils).maybeAddAsyncListeners(futureIteratorEntity, asyncListeners);
    }

    private EntityMeta buildEntityMeta(PropertyMeta... pms) {
        Map<String, PropertyMeta> propertyMetas = new HashMap<>();
        for (PropertyMeta pm : pms) {
            propertyMetas.put(pm.getPropertyName(), pm);
        }
        when(meta.getPropertyMetas()).thenReturn(propertyMetas);
        return meta;
    }

    private void initTypedQuery(RegularStatement regularStatement, EntityMeta meta, Map<String, PropertyMeta> propertyMetas, EntityState entityState) {
        typedQuery = new TypedQuery<>(entityClass, daoContext, configContext, regularStatement, meta, contextFactory, entityState, new Object[] { "a" });

        Whitebox.setInternalState(typedQuery, Map.class, propertyMetas);
        Whitebox.setInternalState(typedQuery, EntityMapper.class, mapper);
        Whitebox.setInternalState(typedQuery, PersistenceContextFactory.class, contextFactory);
        Whitebox.setInternalState(typedQuery, EntityProxifier.class, proxifier);
        Whitebox.setInternalState(typedQuery, AsyncUtils.class, asyncUtils);
        Whitebox.setInternalState(typedQuery, ExecutorService.class, executorService);
    }

}
