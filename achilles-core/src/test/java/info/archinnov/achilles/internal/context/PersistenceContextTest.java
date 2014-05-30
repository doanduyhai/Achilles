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

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.interceptor.Event.POST_LOAD;
import static info.archinnov.achilles.interceptor.Event.POST_PERSIST;
import static info.archinnov.achilles.interceptor.Event.POST_REMOVE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_PERSIST;
import static info.archinnov.achilles.interceptor.Event.PRE_REMOVE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static info.archinnov.achilles.type.Options.CASCondition;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityInitializer;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityPersister;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityRefresher;
import info.archinnov.achilles.internal.persistence.operations.EntityUpdater;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceContextTest {

    private PersistenceContext context;

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

    @Mock
    private DirtyCheckChangeSet changeSet;

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

        context = new PersistenceContext(meta, configurationContext, daoContext, flushContext, CompleteBean.class,
                primaryKey, OptionsBuilder.noOptions());

        Whitebox.setInternalState(context, "initializer", initializer);
        Whitebox.setInternalState(context, "persister", persister);
        Whitebox.setInternalState(context, "proxifier", proxifier);
        Whitebox.setInternalState(context, "refresher", refresher);
        Whitebox.setInternalState(context, "loader", loader);
        Whitebox.setInternalState(context, "updater", updater);

        when(invoker.getPrimaryKey(any(), eq(idMeta))).thenReturn(primaryKey);
    }

    @Test
    public void should_call_flush() throws Exception {
        context.persistenceManagerFacade.flush();

        verify(flushContext).flush();
    }

    @Test
    public void should_duplicate_for_new_entity() throws Exception {
        CompleteBean entity = new CompleteBean();
        entity.setId(primaryKey);
        when(meta.getPrimaryKey(entity)).thenReturn(primaryKey);
        when(flushContext.duplicate()).thenReturn(flushContext);

        PersistenceContext duplicateContext = context.duplicate(entity);

        assertThat(duplicateContext.stateHolderFacade.getEntity()).isSameAs(entity);
        assertThat(duplicateContext.stateHolderFacade.getPrimaryKey()).isSameAs(primaryKey);
    }

    @Test
    public void should_eager_load_entity() throws Exception {
        Row row = mock(Row.class);
        when(daoContext.loadEntity(context.daoFacade)).thenReturn(row);

        assertThat(context.entityFacade.loadEntity()).isSameAs(row);
    }

    @Test
    public void should_load_property() throws Exception {
        Row row = mock(Row.class);
        when(daoContext.loadProperty(context.daoFacade, idMeta)).thenReturn(row);

        assertThat(context.entityFacade.loadProperty(idMeta)).isSameAs(row);
    }

    @Test
    public void should_push_insert() throws Exception {
        //Given
        EntityMeta meta = new EntityMeta();
        List<PropertyMeta> pms = new ArrayList<>();
        meta.setAllMetasExceptIdAndCounters(pms);
        context.entityMeta = meta;
        when(configurationContext.getInsertStrategy()).thenReturn(ALL_FIELDS);

        //When
        context.entityFacade.pushInsertStatement();

        //Then
        verify(daoContext).pushInsertStatement(context.daoFacade, pms);
    }

    @Test
    public void should_push_update() throws Exception {
        List<PropertyMeta> pms = Arrays.asList();
        context.entityFacade.pushUpdateStatement(pms);

        verify(daoContext).pushUpdateStatement(context.daoFacade, pms);
    }

    @Test
    public void should_push_for_collection_and_map_update() throws Exception {
        context.entityFacade.pushCollectionAndMapUpdateStatements(changeSet);

        verify(daoContext).pushCollectionAndMapUpdateStatement(context.daoFacade, changeSet);
    }

    @Test
    public void should_bind_for_removal() throws Exception {
        context.entityFacade.bindForRemoval("table");

        verify(daoContext).bindForRemoval(context.daoFacade, "table");
    }


    // Simple counter
    @Test
    public void should_bind_for_simple_counter_increment() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        context.entityFacade.bindForSimpleCounterIncrement(counterMeta, 11L);

        verify(daoContext).bindForSimpleCounterIncrement(context.daoFacade, counterMeta, 11L);
    }

    @Test
    public void should_get_simple_counter() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        Row row = mock(Row.class);
        when(daoContext.getSimpleCounter(context.daoFacade, counterMeta, LOCAL_QUORUM)).thenReturn(row);
        when(row.getLong(CQL_COUNTER_VALUE)).thenReturn(11L);
        Long counterValue = context.entityFacade.getSimpleCounter(counterMeta, LOCAL_QUORUM);

        assertThat(counterValue).isEqualTo(11L);
    }

    @Test
    public void should_return_null_when_no_simple_counter_value() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        when(daoContext.getSimpleCounter(context.daoFacade, counterMeta, LOCAL_QUORUM)).thenReturn(null);

        assertThat(context.entityFacade.getSimpleCounter(counterMeta, LOCAL_QUORUM)).isNull();
    }

    @Test
    public void should_bind_for_simple_counter_removal() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        context.entityFacade.bindForSimpleCounterRemoval(counterMeta);

        verify(daoContext).bindForSimpleCounterDelete(context.daoFacade, counterMeta);
    }

    // Clustered counter
    @Test
    public void should_push_clustered_counter_increment() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        context.entityFacade.pushClusteredCounterIncrementStatement(counterMeta, 11L);

        verify(daoContext).pushClusteredCounterIncrementStatement(context.daoFacade, counterMeta, 11L);
    }


    @Test
    public void should_get_clustered_counter() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();
        counterMeta.setPropertyName("count");
        Long counterValue = 11L;

        when(daoContext.getClusteredCounterColumn(context.daoFacade, counterMeta)).thenReturn(counterValue);

        Long actual = context.entityFacade.getClusteredCounterColumn(counterMeta);

        assertThat(actual).isEqualTo(counterValue);
    }

    @Test
    public void should_return_null_when_no_clustered_counter_value() throws Exception {

        when(daoContext.getClusteredCounter(context.daoFacade)).thenReturn(null);

        assertThat(context.entityFacade.getClusteredCounter()).isNull();
    }

    @Test
    public void should_bind_for_clustered_counter_removal() throws Exception {
        context.entityFacade.bindForClusteredCounterRemoval();

        verify(daoContext).bindForClusteredCounterDelete(context.daoFacade);
    }

    @Test
    public void should_push_statement_wrapper() throws Exception {
        BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);

        context.daoFacade.pushStatement(bsWrapper);

        verify(flushContext).pushStatement(bsWrapper);
    }

    @Test
    public void should_push_counter_statement_wrapper() throws Exception {
        BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);

        context.daoFacade.pushCounterStatement(bsWrapper);

        verify(flushContext).pushCounterStatement(bsWrapper);
    }

    @Test
    public void should_execute_immediate() throws Exception {
        // Given
        BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);
        ResultSet resultSet = mock(ResultSet.class);

        // When
        when(flushContext.executeImmediate(bsWrapper)).thenReturn(resultSet);

        ResultSet actual = context.daoFacade.executeImmediate(bsWrapper);

        // Then
        assertThat(actual).isSameAs(resultSet);
    }

    @Test
    public void should_persist() throws Exception {
        //Given
        Object entity = new Object();
        context.entity = entity;
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.entityFacade)).thenReturn(entity);

        //When
        Object actual = context.persistenceManagerFacade.persist(entity);

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
        context.persistenceManagerFacade.update(entity);

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
        context.persistenceManagerFacade.remove();

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
        CompleteBean found = context.persistenceManagerFacade.find(CompleteBean.class);

        //Then
        assertThat(found).isSameAs(entity);
        verify(flushContext).triggerInterceptor(meta, entity, POST_LOAD);
    }

    @Test
    public void should_return_null_when_not_found() throws Exception {
        when(loader.load(context.entityFacade, CompleteBean.class)).thenReturn(null);

        CompleteBean found = context.persistenceManagerFacade.find(CompleteBean.class);

        assertThat(found).isNull();
        verifyZeroInteractions(proxifier);
    }

    @Test
    public void should_get_proxy() throws Exception {
        when(loader.createEmptyEntity(context.entityFacade, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxyWithNoFieldLoaded(entity, context.entityFacade)).thenReturn(entity);

        CompleteBean found = context.persistenceManagerFacade.getProxy(CompleteBean.class);

        assertThat(found).isSameAs(entity);
    }

    @Test
    public void should_refresh() throws Exception {
        context.persistenceManagerFacade.refresh(entity);

        InOrder inOrder = Mockito.inOrder(flushContext, refresher);

        inOrder.verify(refresher).refresh(entity, context.entityFacade);
        inOrder.verify(flushContext).triggerInterceptor(meta, context.entity, POST_LOAD);
    }

    @Test
    public void should_initialize() throws Exception {
        CompleteBean actual = context.persistenceManagerFacade.initialize(entity);

        assertThat(actual).isSameAs(entity);

        verify(initializer).initializeEntity(entity, meta);
    }

    @Test
    public void should_get_cas_conditions() throws Exception {
        //Given
        final CASCondition CASCondition = new CASCondition("name", "John");
        context.options = OptionsBuilder.ifConditions(CASCondition);

        //When
        final List<CASCondition> CASConditions = context.stateHolderFacade.getCasConditions();

        //Then
        assertThat(CASConditions).containsExactly(CASCondition);
    }

    @Test
    public void should_return_empty_list_for_cas_conditions() throws Exception {
        //Given
        context.options = OptionsBuilder.noOptions();

        //When
        final List<CASCondition> CASConditions = context.stateHolderFacade.getCasConditions();

        //Then
        assertThat(CASConditions).isNotNull().isEmpty();
    }
}
