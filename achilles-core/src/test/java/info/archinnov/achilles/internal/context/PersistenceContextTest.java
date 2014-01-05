/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityInitializer;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityUpdater;
import info.archinnov.achilles.internal.persistence.operations.EntityPersister;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityRefresher;
import info.archinnov.achilles.internal.proxy.EntityInterceptor;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

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

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private EntityMeta meta;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PropertyMeta idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp() throws Exception {
		when(meta.getIdMeta()).thenReturn(idMeta);
		when(meta.<CompleteBean> getEntityClass()).thenReturn(CompleteBean.class);
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
	public void should_return_is_clustered_true() throws Exception {
		when(meta.isClusteredEntity()).thenReturn(true);
		assertThat(context.isClusteredEntity()).isTrue();
	}

	@Test
	public void should_return_column_family_name() throws Exception {
		when(meta.getTableName()).thenReturn("table");
		assertThat(context.getTableName()).isEqualTo("table");
	}

	@Test
	public void should_return_true_for_is_batch_mode() throws Exception {
		when(flushContext.type()).thenReturn(AbstractFlushContext.FlushType.BATCH);
		assertThat(context.isBatchMode()).isTrue();
	}

	@Test
	public void should_return_false_for_is_batch_mode() throws Exception {
		when(flushContext.type()).thenReturn(AbstractFlushContext.FlushType.IMMEDIATE);
		assertThat(context.isBatchMode()).isFalse();
	}

	@Test
	public void should_call_flush() throws Exception {
		context.flush();

		verify(flushContext).flush();
	}

	@Test
	public void should_call_end_batch() throws Exception {
		context.endBatch();

		verify(flushContext).endBatch();
	}

	@Test
	public void should_duplicate_for_new_entity() throws Exception {
		CompleteBean entity = new CompleteBean();
		entity.setId(primaryKey);
		when(meta.getPrimaryKey(entity)).thenReturn(primaryKey);
        when(flushContext.duplicate()).thenReturn(flushContext);

		PersistenceContext duplicateContext = context.duplicate(entity);

		assertThat(duplicateContext.getEntity()).isSameAs(entity);
		assertThat(duplicateContext.getPrimaryKey()).isSameAs(primaryKey);
	}

	@Test
	public void should_eager_load_entity() throws Exception {
		Row row = mock(Row.class);
		when(daoContext.eagerLoadEntity(context)).thenReturn(row);

		assertThat(context.eagerLoadEntity()).isSameAs(row);
	}

	@Test
	public void should_load_property() throws Exception {
		Row row = mock(Row.class);
		when(daoContext.loadProperty(context, idMeta)).thenReturn(row);

		assertThat(context.loadProperty(idMeta)).isSameAs(row);
	}

	@Test
	public void should_bind_for_insert() throws Exception {
		context.pushInsertStatement();

		verify(daoContext).pushInsertStatement(context);
	}

	@Test
	public void should_bind_for_update() throws Exception {
		List<PropertyMeta> pms = Arrays.asList();
		context.pushUpdateStatement(pms);

		verify(daoContext).pushUpdateStatement(context, pms);
	}

	@Test
	public void should_bind_for_removal() throws Exception {
		context.bindForRemoval("table");

		verify(daoContext).bindForRemoval(context, "table");
	}

	@Test
	public void should_bind_and_execute() throws Exception {
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);

		when(daoContext.bindAndExecute(ps, 11L, "a")).thenReturn(rs);
		ResultSet actual = context.bindAndExecute(ps, 11L, "a");

		assertThat(actual).isSameAs(rs);
	}

	// Simple counter
	@Test
	public void should_bind_for_simple_counter_increment() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.bindForSimpleCounterIncrement(counterMeta, 11L);

		verify(daoContext).bindForSimpleCounterIncrement(context, meta, counterMeta, 11L);
	}

	@Test
	public void should_increment_simple_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.incrementSimpleCounter(counterMeta, 11L, LOCAL_QUORUM);

		verify(daoContext).incrementSimpleCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_decrement_simple_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.decrementSimpleCounter(counterMeta, 11L, LOCAL_QUORUM);

		verify(daoContext).decrementSimpleCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_get_simple_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		Row row = mock(Row.class);
		when(daoContext.getSimpleCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(row);
		when(row.getLong(CQL_COUNTER_VALUE)).thenReturn(11L);
		Long counterValue = context.getSimpleCounter(counterMeta, LOCAL_QUORUM);

		assertThat(counterValue).isEqualTo(11L);
	}

	@Test
	public void should_return_null_when_no_simple_counter_value() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		when(daoContext.getSimpleCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(null);

		assertThat(context.getSimpleCounter(counterMeta, LOCAL_QUORUM)).isNull();
	}

	@Test
	public void should_bind_for_simple_counter_removal() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.bindForSimpleCounterRemoval(counterMeta);

		verify(daoContext).bindForSimpleCounterDelete(context, meta, counterMeta, entity.getId());
	}

	// Clustered counter
	@Test
	public void should_bind_for_clustered_counter_increment() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.pushClusteredCounterIncrementStatement(counterMeta, 11L);

		verify(daoContext).pushClusteredCounterIncrementStatement(context, meta, counterMeta, 11L);
	}

	@Test
	public void should_increment_clustered_counter() throws Exception {
		context.incrementClusteredCounter(11L, LOCAL_QUORUM);

		verify(daoContext).incrementClusteredCounter(context, meta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_decrement_clustered_counter() throws Exception {
		context.decrementClusteredCounter(11L, LOCAL_QUORUM);

		verify(daoContext).decrementClusteredCounter(context, meta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_get_clustered_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();
		counterMeta.setPropertyName("count");

		Row row = mock(Row.class);
		when(daoContext.getClusteredCounter(context, LOCAL_QUORUM)).thenReturn(row);
		when(row.getLong("count")).thenReturn(11L);
		Long counterValue = context.getClusteredCounter(counterMeta, LOCAL_QUORUM);

		assertThat(counterValue).isEqualTo(11L);
	}

	@Test
	public void should_return_null_when_no_clustered_counter_value() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		when(daoContext.getClusteredCounter(context, LOCAL_QUORUM)).thenReturn(null);

		assertThat(context.getClusteredCounter(counterMeta, LOCAL_QUORUM)).isNull();
	}

	@Test
	public void should_bind_for_clustered_counter_removal() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.bindForClusteredCounterRemoval(counterMeta);

		verify(daoContext).bindForClusteredCounterDelete(context, meta, counterMeta, entity.getId());
	}

	@Test
	public void should_push_statement_wrapper() throws Exception {
		BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);

		context.pushStatement(bsWrapper);

		verify(flushContext).pushStatement(bsWrapper);
	}

	@Test
	public void should_execute_immediate() throws Exception {
		// Given
		BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);
		ResultSet resultSet = mock(ResultSet.class);

		// When
		when(flushContext.executeImmediate(bsWrapper)).thenReturn(resultSet);

		ResultSet actual = context.executeImmediate(bsWrapper);

		// Then
		assertThat(actual).isSameAs(resultSet);
	}

	@Test
	public void should_persist() throws Exception {
        //Given
        Object entity = new Object();
        context.entity = entity;
        when(proxifier.buildProxyWithAllFieldsLoaded(entity,context)).thenReturn(entity);

        //When
		Object actual = context.persist(entity);

        //Then
        assertThat(actual).isSameAs(entity);

        InOrder inOrder = Mockito.inOrder(flushContext,persister);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_PERSIST);
        inOrder.verify(persister).persist(context);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_PERSIST);
	}

	@Test
	public void should_update() throws Exception {
        //Given
        final CompleteBean rawEntity = new CompleteBean();
        context.entity = rawEntity;

        //When
		context.update(entity);

        //Then
        InOrder inOrder = Mockito.inOrder(flushContext, updater);

        inOrder.verify(flushContext).triggerInterceptor(meta,rawEntity, PRE_UPDATE);
        inOrder.verify(updater).update(context,entity);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, rawEntity, POST_UPDATE);
    }

	@Test
	public void should_remove() throws Exception {
        //Given
        Object entity = new Object();
        context.entity = entity;

        //When
		context.remove();

        //Then
        InOrder inOrder = Mockito.inOrder(flushContext,persister);

        inOrder.verify(flushContext).triggerInterceptor(meta, entity, PRE_REMOVE);
        inOrder.verify(persister).remove(context);
        inOrder.verify(flushContext).flush();
        inOrder.verify(flushContext).triggerInterceptor(meta, entity, POST_REMOVE);
    }

	@Test
	public void should_find() throws Exception {
        //Given
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxyWithEagerFieldsLoaded(entity, context)).thenReturn(entity);

        //When
		CompleteBean found = context.find(CompleteBean.class);

        //Then
		assertThat(found).isSameAs(entity);
        verify(flushContext).triggerInterceptor(meta, entity, POST_LOAD);
    }

	@Test
	public void should_return_null_when_not_found() throws Exception {
		when(loader.load(context, CompleteBean.class)).thenReturn(null);

		CompleteBean found = context.find(CompleteBean.class);

		assertThat(found).isNull();
		verifyZeroInteractions(proxifier);
	}

	@Test
	public void should_get_reference() throws Exception {
		when(loader.createEmptyEntity(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxyWithNoFieldLoaded(entity, context)).thenReturn(entity);

		CompleteBean found = context.getProxy(CompleteBean.class);

		assertThat(found).isSameAs(entity);
	}

	@Test
	public void should_refresh() throws Exception {
		context.refresh(entity);
		verify(refresher).refresh(entity,context);
	}

	@Test
	public void should_initialize() throws Exception {
		@SuppressWarnings("unchecked")
		EntityInterceptor<CompleteBean> interceptor = mock(EntityInterceptor.class);

		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);

		CompleteBean actual = context.initialize(entity);

		assertThat(actual).isSameAs(entity);

		verify(initializer).initializeEntity(entity, meta, interceptor);
	}
}
