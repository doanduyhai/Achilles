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
package info.archinnov.achilles.context;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceContextTest {
	@InjectMocks
	private CQLPersistenceContext context;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private CQLAbstractFlushContext<?> flushContext;

	@Mock
	private ConfigurationContext configurationContext;

	@Mock
	private CQLEntityLoader loader;

	@Mock
	private CQLEntityPersister persister;

	@Mock
	private CQLEntityMerger merger;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private EntityInitializer initializer;

	@Mock
	private EntityRefresher<CQLPersistenceContext> refresher;

	@Mock
	private ReflectionInvoker invoker;

	private EntityMeta meta;

	private PropertyMeta idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp() throws Exception {
		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).accessors()
				.invoker(invoker).build();

		meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setEntityClass(CompleteBean.class);

		Whitebox.setInternalState(context, "entityMeta", meta);
		Whitebox.setInternalState(context, "primaryKey", entity.getId());
		Whitebox.setInternalState(context, CQLEntityLoader.class, loader);
		Whitebox.setInternalState(context, CQLEntityMerger.class, merger);
		Whitebox.setInternalState(context, CQLEntityPersister.class, persister);
		Whitebox.setInternalState(context, EntityRefresher.class, refresher);
		Whitebox.setInternalState(context, CQLEntityProxifier.class, proxifier);
		Whitebox.setInternalState(context, "initializer", initializer);
		Whitebox.setInternalState(context, "options", OptionsBuilder.noOptions());
		Whitebox.setInternalState(context, CQLAbstractFlushContext.class, flushContext);

		when(invoker.getPrimaryKey(any(), eq(idMeta))).thenReturn(primaryKey);
	}

	@Test
	public void should_duplicate_for_new_entity() throws Exception {
		CompleteBean entity = new CompleteBean();
		entity.setId(primaryKey);
		CQLPersistenceContext duplicateContext = context.duplicate(entity);

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
		PropertyMeta counterMeta = new PropertyMeta();

		context.incrementClusteredCounter(counterMeta, 11L, LOCAL_QUORUM);

		verify(daoContext).incrementClusteredCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_decrement_clustered_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.decrementClusteredCounter(counterMeta, 11L, LOCAL_QUORUM);

		verify(daoContext).decrementClusteredCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
	}

	@Test
	public void should_get_clustered_counter() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();
		counterMeta.setPropertyName("count");

		Row row = mock(Row.class);
		when(daoContext.getClusteredCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(row);
		when(row.getLong("count")).thenReturn(11L);
		Long counterValue = context.getClusteredCounter(counterMeta, LOCAL_QUORUM);

		assertThat(counterValue).isEqualTo(11L);
	}

	@Test
	public void should_return_null_when_no_clustered_counter_value() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		when(daoContext.getClusteredCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(null);

		assertThat(context.getClusteredCounter(counterMeta, LOCAL_QUORUM)).isNull();
	}

	@Test
	public void should_bind_for_clustered_counter_removal() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		context.bindForClusteredCounterRemoval(counterMeta);

		verify(daoContext).bindForClusteredCounterDelete(context, meta, counterMeta, entity.getId());
	}

	@Test
	public void should_push_bound_statement() throws Exception {
		BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);

		context.pushBoundStatement(bsWrapper, EACH_QUORUM);

		verify(flushContext).pushBoundStatement(bsWrapper, EACH_QUORUM);
	}

	@Test
	public void should_push_statement() throws Exception {
		Statement statement = mock(Statement.class);

		context.pushStatement(statement, EACH_QUORUM);

		verify(flushContext).pushStatement(statement, EACH_QUORUM);
	}

	@Test
	public void should_execute_immediate_with_consistency() throws Exception {
		BoundStatementWrapper bsWrapper = mock(BoundStatementWrapper.class);
		BoundStatement bs = mock(BoundStatement.class);

		Object[] boundValues = new Object[1];
		when(bsWrapper.getBs()).thenReturn(bs);
		when(bsWrapper.getValues()).thenReturn(boundValues);

		ResultSet resultSet = mock(ResultSet.class);
		when(flushContext.executeImmediateWithConsistency(bs, EACH_QUORUM, boundValues)).thenReturn(resultSet);

		ResultSet actual = context.executeImmediateWithConsistency(bsWrapper, EACH_QUORUM);

		assertThat(actual).isSameAs(resultSet);
	}

	@Test
	public void should_persist() throws Exception {
		context.persist();
		verify(persister).persist(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_merge() throws Exception {
		when(merger.merge(context, entity)).thenReturn(entity);

		CompleteBean merged = context.merge(entity);

		assertThat(merged).isSameAs(entity);
		verify(flushContext).flush();
	}

	@Test
	public void should_remove() throws Exception {
		context.remove();
		verify(persister).remove(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_find() throws Exception {
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean found = context.find(CompleteBean.class);

		assertThat(found).isSameAs(entity);
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
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean found = context.getReference(CompleteBean.class);

		assertThat(context.isLoadEagerFields()).isFalse();
		assertThat(found).isSameAs(entity);
	}

	@Test
	public void should_refresh() throws Exception {
		context.refresh();
		verify(refresher).refresh(context);
	}

	@Test
	public void should_initialize() throws Exception {
		@SuppressWarnings("unchecked")
		EntityInterceptor<CQLPersistenceContext, CompleteBean> interceptor = mock(EntityInterceptor.class);

		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);

		CompleteBean actual = context.initialize(entity);

		assertThat(actual).isSameAs(entity);

		verify(initializer).initializeEntity(entity, meta, interceptor);
	}
}
