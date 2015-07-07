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

package info.archinnov.achilles.internal.context;

import static info.archinnov.achilles.interceptor.Event.POST_LOAD;
import static info.archinnov.achilles.interceptor.Event.POST_INSERT;
import static info.archinnov.achilles.interceptor.Event.POST_DELETE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_INSERT;
import static info.archinnov.achilles.interceptor.Event.PRE_DELETE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.ImmediateValue;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityInitializer;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityPersister;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityRefresher;
import info.archinnov.achilles.internal.persistence.operations.EntityUpdater;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.options.Options;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerFacadeTest {

    private PersistenceContext context;

    private PersistenceContext.PersistenceManagerFacade facade;

    @Mock
    private DaoContext daoContext;

    @Mock
    private ConfigurationContext configurationContext;

    @Mock
    private EntityLoader loader;

    @Mock
    private EntityPersister persister;

    @Mock
    private EntityUpdater updater;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityProxifier proxifier;

    @Mock
    private EntityInitializer initializer;

    @Mock
    private EntityRefresher refresher;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private AbstractFlushContext flushContext;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private ListenableFuture<List<ResultSet>> futureResultSets;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    @Captor
    private ArgumentCaptor<Function<List<ResultSet>, CompleteBean>> resultSetsToEntityCaptor;

    @Captor
    private ArgumentCaptor<Function<CompleteBean, CompleteBean>> isoEntityCaptor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock
    private Options options;

    private Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

    @Before
    public void setUp() throws Exception {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(configurationContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);
        when(configurationContext.getExecutorService()).thenReturn(executorService);
        when(flushContext.flush()).thenReturn(futureResultSets);

        context = new PersistenceContext(meta, configurationContext, daoContext, flushContext, CompleteBean.class, primaryKey, options);
        facade = context.persistenceManagerFacade;

        context.flushContext = flushContext;
        context.initializer = initializer;
        context.persister = persister;
        context.proxifier = proxifier;
        context.refresher = refresher;
        context.loader = loader;
        context.updater = updater;
        context.asyncUtils = asyncUtils;
        context.configContext = configurationContext;
        context.entity = entity;

    }

    @Test
    public void should_persist() throws Exception {
        //Given
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);
        when(asyncUtils.transformFuture(eq(futureResultSets), resultSetsToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.transformFuture(eq(futureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        //When
        final AchillesFuture<CompleteBean> actual = facade.persist(entity);

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(resultSetsToEntityCaptor.getValue().apply(null)).isSameAs(entity);
        assertThat(isoEntityCaptor.getValue().apply(null)).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, persister, asyncUtils);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_INSERT);
        inOrder.verify(persister).persist(context.entityFacade);
        inOrder.verify(flushContext).flush();
        inOrder.verify(asyncUtils).maybeAddAsyncListeners(futureEntity, options);
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_INSERT);
    }

    @Test
    public void should_batch_persist() throws Exception {
        //Given
        final ArgumentCaptor<ImmediateValue> immediateValueCaptor = ArgumentCaptor.forClass(ImmediateValue.class);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);
        when(asyncUtils.buildInterruptible(immediateValueCaptor.capture())).thenReturn(achillesFutureEntity);

        //When
        final AchillesFuture<CompleteBean> actual = facade.batchInsert(entity);

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);
        assertThat(immediateValueCaptor.getValue().get()).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, persister, asyncUtils);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_INSERT);
        inOrder.verify(persister).persist(context.entityFacade);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_INSERT);

    }

    @Test
    public void should_update() throws Exception {
        //Given
        when(asyncUtils.transformFuture(eq(futureResultSets), resultSetsToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);
        Map<Method, DirtyChecker> dirtyMap = mock(Map.class);
        when(proxifier.getInterceptor(entity).getDirtyMap()).thenReturn(dirtyMap);

        //When
        final AchillesFuture<CompleteBean> actual = facade.update(entity);

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);
        assertThat(resultSetsToEntityCaptor.getValue().apply(null)).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, updater, dirtyMap, asyncUtils);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_UPDATE);
        inOrder.verify(updater).update(context.entityFacade, entity);
        inOrder.verify(flushContext).flush();
        inOrder.verify(asyncUtils).maybeAddAsyncListeners(futureEntity, options);
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_UPDATE);
        inOrder.verify(dirtyMap).clear();
    }

    @Test
    public void should_delete() throws Exception {
        //Given
        when(asyncUtils.transformFuture(eq(futureResultSets), resultSetsToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        //When
        final AchillesFuture<CompleteBean> actual = facade.delete();

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);
        assertThat(resultSetsToEntityCaptor.getValue().apply(null)).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, persister, asyncUtils);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_DELETE);
        inOrder.verify(persister).delete(context.entityFacade);
        inOrder.verify(flushContext).flush();
        inOrder.verify(asyncUtils).maybeAddAsyncListeners(futureEntity, options);
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_DELETE);
    }

    @Test
    public void should_find() throws Exception {
        //Given
        when(loader.load(context.entityFacade, CompleteBean.class)).thenReturn(achillesFutureEntity);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);

        when(asyncUtils.transformFuture(eq(achillesFutureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.transformFuture(eq(futureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        //When
        final AchillesFuture<CompleteBean> actual = facade.find(CompleteBean.class);

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(isoEntityCaptor.getAllValues().get(0).apply(entity)).isSameAs(entity);
        assertThat(isoEntityCaptor.getAllValues().get(1).apply(entity)).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, updater, asyncUtils);
        inOrder.verify(asyncUtils).maybeAddAsyncListeners(futureEntity, options);
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_LOAD);
    }

    @Test
    public void should_get_proxy() throws Exception {
        // Given
        when(loader.createEmptyEntity(context.entityFacade, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxyWithNoFieldLoaded(entity, context.entityFacade)).thenReturn(entity);

        // When
        final CompleteBean actual = facade.getProxy(CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(entity);

    }

    @Test
    public void should_refresh() throws Exception {
        // Given
        CompleteBean proxy = new CompleteBean();
        when(refresher.refresh(proxy, context.entityFacade)).thenReturn(achillesFutureEntity);
        when(proxifier.removeProxy(proxy)).thenReturn(entity);
        when(asyncUtils.transformFuture(eq(achillesFutureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.transformFuture(eq(futureEntity), isoEntityCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = facade.refresh(proxy);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(isoEntityCaptor.getAllValues().get(0).apply(proxy)).isSameAs(entity);
        assertThat(isoEntityCaptor.getAllValues().get(1).apply(entity)).isSameAs(entity);

        InOrder inOrder = inOrder(flushContext, refresher, proxifier, asyncUtils);

        inOrder.verify(refresher).refresh(proxy, context.entityFacade);
        inOrder.verify(asyncUtils).maybeAddAsyncListeners(futureEntity, options);
        inOrder.verify(proxifier).removeProxy(proxy);
        inOrder.verify(flushContext).triggerInterceptor(meta, context.entity, POST_LOAD);
    }

    @Test
    public void should_initialize() throws Exception {
        CompleteBean actual = facade.initialize(entity);

        assertThat(actual).isSameAs(entity);

        verify(initializer).initializeEntity(entity, meta);
    }


    @Test
    public void should_initialize_list() throws Exception {
        List<CompleteBean> actual = facade.initialize(asList(entity));

        assertThat(actual).containsExactly(entity);

        verify(initializer).initializeEntity(entity, meta);
    }

    @Test
    public void should_initialize_set() throws Exception {
        Set<CompleteBean> actual = facade.initialize(Sets.newHashSet(entity));

        assertThat(actual).containsExactly(entity);

        verify(initializer).initializeEntity(entity, meta);
    }
}
