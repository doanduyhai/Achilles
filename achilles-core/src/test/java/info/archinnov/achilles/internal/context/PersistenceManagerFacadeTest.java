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
import static info.archinnov.achilles.interceptor.Event.POST_PERSIST;
import static info.archinnov.achilles.interceptor.Event.POST_REMOVE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_PERSIST;
import static info.archinnov.achilles.interceptor.Event.PRE_REMOVE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.collect.Sets;
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
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerFacadeTest {

    private PersistenceContext context;

    private PersistenceContext.PersistenceManagerFacade facade;

    @Mock
    private DaoContext daoContext;

    @Mock
    private AbstractFlushContext flushContext;

    @Mock
    private ConfigurationContext configurationContext;

    @Mock
    private EntityLoader loader;

    @Mock
    private EntityPersister persister;

    @Mock
    private EntityUpdater updater;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private EntityInitializer initializer;

    @Mock
    private EntityRefresher refresher;

    @Mock
    private ReflectionInvoker invoker;


    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private Long primaryKey = RandomUtils.nextLong();

    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

    @Before
    public void setUp() throws Exception {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(configurationContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);

        context = new PersistenceContext(meta, configurationContext, daoContext, flushContext, CompleteBean.class, primaryKey, OptionsBuilder.noOptions());
        facade = context.persistenceManagerFacade;

        Whitebox.setInternalState(context, "initializer", initializer);
        Whitebox.setInternalState(context, "persister", persister);
        Whitebox.setInternalState(context, "proxifier", proxifier);
        Whitebox.setInternalState(context, "refresher", refresher);
        Whitebox.setInternalState(context, "loader", loader);
        Whitebox.setInternalState(context, "updater", updater);
    }

    @Test
    public void should_persist() throws Exception {
        //Given
        Object entity = new Object();
        context.entity = entity;
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);

        //When
        Object actual = facade.persist(entity);

        //Then
        assertThat(actual).isSameAs(entity);

        InOrder inOrder = Mockito.inOrder(flushContext, persister);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_PERSIST);
        inOrder.verify(persister).persist(context.entityFacade);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_PERSIST);
    }

    @Test
    public void should_update() throws Exception {
        //Given
        final CompleteBean rawEntity = new CompleteBean();
        context.entity = rawEntity;

        //When
        facade.update(entity);

        //Then
        InOrder inOrder = Mockito.inOrder(flushContext, updater);

        inOrder.verify(flushContext).triggerInterceptor(meta, rawEntity, PRE_UPDATE);
        inOrder.verify(updater).update(context.entityFacade, entity);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, rawEntity, POST_UPDATE);
    }

    @Test
    public void should_remove() throws Exception {
        //Given
        Object entity = new Object();
        context.entity = entity;

        //When
        facade.remove();

        //Then
        InOrder inOrder = Mockito.inOrder(flushContext, persister);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_REMOVE);
        inOrder.verify(persister).remove(context.entityFacade);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_REMOVE);
    }

    @Test
    public void should_find() throws Exception {
        //Given
        when(loader.load(context.entityFacade, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);

        //When
        CompleteBean found = facade.find(CompleteBean.class);

        //Then
        assertThat(found).isSameAs(entity);
        verify(flushContext).triggerInterceptor(meta, entity, POST_LOAD);
    }

    @Test
    public void should_return_null_when_not_found() throws Exception {
        when(loader.load(context.entityFacade, CompleteBean.class)).thenReturn(null);

        CompleteBean found = facade.find(CompleteBean.class);

        assertThat(found).isNull();
        verifyZeroInteractions(proxifier);
    }

    @Test
    public void should_get_proxy() throws Exception {
        when(loader.createEmptyEntity(context.entityFacade, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxyWithNoFieldLoaded(entity, context.entityFacade)).thenReturn(entity);

        CompleteBean found = facade.getProxy(CompleteBean.class);

        assertThat(found).isSameAs(entity);
    }

    @Test
    public void should_refresh() throws Exception {
        facade.refresh(entity);

        InOrder inOrder = Mockito.inOrder(flushContext, refresher);

        inOrder.verify(refresher).refresh(entity, context.entityFacade);
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
