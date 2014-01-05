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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.StatementGenerator;
import info.archinnov.achilles.statement.cache.CacheManager;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.statement.prepared.PreparedStatementBinder;
import info.archinnov.achilles.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.type.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Insert.Options;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.datastax.driver.core.querybuilder.Using;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class DaoContextTest {
    private final Optional<Integer> ttlO = Optional.fromNullable(null);

    @Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private DaoContext daoContext;

	@Mock
	private StatementGenerator statementGenerator;

	@Mock
	private Map<Class<?>, PreparedStatement> insertPSs;

	@Mock
	private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;

	@Mock
	private Map<Class<?>, PreparedStatement> selectEagerPSs;

	@Mock
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;

	@Mock
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;

	private Map<Class<?>, Map<CQLQueryType, PreparedStatement>> clusteredCounterQueryMap = new HashMap<Class<?>, Map<CQLQueryType, PreparedStatement>>();

	@Mock
	private Session session;

	@Mock
	private PreparedStatementBinder binder;

	@Mock
	private CacheManager cacheManager;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private Insert insert;

	@Mock
	private Update.Where update;

	@Mock
	private PreparedStatement ps;

	@Mock
	private BoundStatementWrapper bsWrapper;

	@Mock
	private BoundStatement bs;

	@Captor
	ArgumentCaptor<Using> usingCaptor;

	@Captor
	ArgumentCaptor<RegularStatementWrapper> statementWrapperCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
	public void setUp() {
		Whitebox.setInternalState(daoContext, PreparedStatementBinder.class, binder);
		Whitebox.setInternalState(daoContext, CacheManager.class, cacheManager);
		Whitebox.setInternalState(daoContext, "insertPSs", insertPSs);
		Whitebox.setInternalState(daoContext, Cache.class, dynamicPSCache);
		Whitebox.setInternalState(daoContext, "selectEagerPSs", selectEagerPSs);
		Whitebox.setInternalState(daoContext, "removePSs", removePSs);
		Whitebox.setInternalState(daoContext, "counterQueryMap", counterQueryMap);
		Whitebox.setInternalState(daoContext, "clusteredCounterQueryMap", clusteredCounterQueryMap);
		Whitebox.setInternalState(daoContext, Session.class, session);
		Whitebox.setInternalState(daoContext, StatementGenerator.class, statementGenerator);
		clusteredCounterQueryMap.clear();
		entityMeta = new EntityMeta();
		entityMeta.setEntityClass(CompleteBean.class);
		entityMeta.setConsistencyLevels(Pair.create(ONE, EACH_QUORUM));

		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.<CompleteBean> getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntity()).thenReturn(entity);
		when(context.getPrimaryKey()).thenReturn(entity.getId());

		insertPSs.clear();
		selectEagerPSs.clear();
		removePSs.clear();
	}

	@Test
	public void should_push_insert() throws Exception {
		// Given
		entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer> absent());
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(null));
		when(insertPSs.get(CompleteBean.class)).thenReturn(ps);
		when(binder.bindForInsert(ps, entityMeta, entity, ALL, ttlO)).thenReturn(bsWrapper);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));

		daoContext.pushInsertStatement(context);

		// Then
		verify(context).pushStatement(bsWrapper);
	}

	@Test
	public void should_push_insert_with_timestamp() throws Exception {
		// Given
		Long timestamp = 115L;
		Options using = insertInto("test").using(timestamp(timestamp));
		Object[] boundValues = new Object[] {};
		Pair<Insert, Object[]> pair = Pair.create(insert, boundValues);
		entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer>fromNullable(null));
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(timestamp));
		when(context.getEntity()).thenReturn(entity);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(statementGenerator.generateInsert(entity, entityMeta)).thenReturn(pair);
		when(insert.using(usingCaptor.capture())).thenReturn(using);

		daoContext.pushInsertStatement(context);

		// Then
		verify(context).pushStatement(statementWrapperCaptor.capture());
		assertThat(statementWrapperCaptor.getValue().getValues()).contains(timestamp);
		assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(new Long(timestamp));
		assertThat(using.getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.ALL);
	}

	@Test
	public void should_push_insert_with_ttl_and_timestamp() throws Exception {
		// Given
		Integer ttl = 115;
		Long timestamp = 115L;
		Options using = insertInto("test").using(ttl(ttl));
		Object[] boundValues = new Object[] {};
		Pair<Insert, Object[]> pair = Pair.create(insert, boundValues);
		entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer> fromNullable(ttl));
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(timestamp));
		when(context.getEntity()).thenReturn(entity);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(statementGenerator.generateInsert(entity, entityMeta)).thenReturn(pair);
		when(insert.using(usingCaptor.capture())).thenReturn(using);
		daoContext.pushInsertStatement(context);

		// Then
		verify(context).pushStatement(statementWrapperCaptor.capture());
		assertThat(statementWrapperCaptor.getValue().getValues()).contains(ttl, timestamp);
		assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(new Long(ttl));

		List<Using> usings = Whitebox.getInternalState(using, "usings");
		assertThat(Whitebox.getInternalState(usings.get(0), "value")).isEqualTo(new Long(timestamp));
		assertThat(using.getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.ALL);
	}

	@Test
	public void should_push_update() throws Exception {
		// Given
		PropertyMeta nameMeta = PropertyMetaTestBuilder.valueClass(String.class).field("name").build();
		PropertyMeta ageMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("age").build();
		List<PropertyMeta> pms = Arrays.asList(nameMeta, ageMeta);

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer> absent());
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(null));
		when(cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms)).thenReturn(ps);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(binder.bindForUpdate(ps, entityMeta, pms, entity, EACH_QUORUM,ttlO)).thenReturn(bsWrapper);

		daoContext.pushUpdateStatement(context, pms);

		// Then
		verify(context).pushStatement(bsWrapper);
	}

	@Test
	public void should_push_update_with_timestamp() throws Exception {
		// Given
		Long timestamp = 15465L;
		Update.Options options = update("test").using(timestamp(timestamp));
		Object[] boundValues = new Object[] {};
		Pair<Where, Object[]> pair = Pair.create(update, boundValues);
		PropertyMeta nameMeta = PropertyMetaTestBuilder.valueClass(String.class).field("name").build();
		PropertyMeta ageMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("age").build();
		List<PropertyMeta> pms = Arrays.asList(nameMeta, ageMeta);

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer>fromNullable(null));
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(timestamp));
		when(context.getEntity()).thenReturn(entity);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(statementGenerator.generateUpdateFields(entity, entityMeta, pms)).thenReturn(pair);
		when(update.using(usingCaptor.capture())).thenReturn(options);

		daoContext.pushUpdateStatement(context, pms);

		// Then
		verify(context).pushStatement(statementWrapperCaptor.capture());
		assertThat(statementWrapperCaptor.getValue().getValues()).contains(timestamp);

		assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(timestamp);
		assertThat(options.getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_push_update_with_ttl_and_timestamp() throws Exception {
		// Given
		Integer ttl = 54321;
		Long timestamp = 15465L;
		Update.Options options = update("test").using(timestamp(timestamp));
		Object[] boundValues = new Object[] {};
		Pair<Where, Object[]> pair = Pair.create(update, boundValues);
		PropertyMeta nameMeta = PropertyMetaTestBuilder.valueClass(String.class).field("name").build();
		PropertyMeta ageMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("age").build();
		List<PropertyMeta> pms = Arrays.asList(nameMeta, ageMeta);

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer> fromNullable(ttl));
		when(context.getTimestamp()).thenReturn(Optional.<Long> fromNullable(timestamp));
		when(context.getEntity()).thenReturn(entity);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(statementGenerator.generateUpdateFields(entity, entityMeta, pms)).thenReturn(pair);
		when(update.using(usingCaptor.capture())).thenReturn(options);

		daoContext.pushUpdateStatement(context, pms);

		// Then
		verify(context).pushStatement(statementWrapperCaptor.capture());
		assertThat(statementWrapperCaptor.getValue().getValues()).contains(ttl, timestamp);
		assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(new Long(timestamp));

		List<Using> usings = Whitebox.getInternalState(options, "usings");
		assertThat(Whitebox.getInternalState(usings.get(1), "value")).isEqualTo(new Long(ttl));
	}

	@Test
	public void should_bind_for_removal() throws Exception {
		when(removePSs.get(CompleteBean.class)).thenReturn(ImmutableMap.of("table", ps));
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId(), EACH_QUORUM)).thenReturn(
				bsWrapper);

		daoContext.bindForRemoval(context, "table");

		verify(context).pushStatement(bsWrapper);
	}

	@Test
	public void should_exception_when_removal_ps_not_found_for_a_table() throws Exception {
		when(removePSs.get(CompleteBean.class)).thenReturn(ImmutableMap.of("some_table", ps));
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot find prepared statement for deletion for table 'table'");

		daoContext.bindForRemoval(context, "table");
	}

	@Test
	public void should_eager_load_entity() throws Exception {
		// Given
		entityMeta.setConsistencyLevels(Pair.create(LOCAL_QUORUM, LOCAL_QUORUM));
		ResultSet resultSet = mock(ResultSet.class);
		Row row = mock(Row.class);

		// When
		when(selectEagerPSs.get(CompleteBean.class)).thenReturn(ps);
		when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId(), LOCAL_QUORUM)).thenReturn(
				bsWrapper);
		when(resultSet.all()).thenReturn(Arrays.asList(row));
		when(context.executeImmediate(bsWrapper)).thenReturn(resultSet);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));

		// Then
		Row actual = daoContext.eagerLoadEntity(context);
		assertThat(actual).isSameAs(row);
	}

	@Test
	public void should_load_property() throws Exception {
		// Given
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
				.consistencyLevels(Pair.create(ONE, ALL)).build();
		ResultSet resultSet = mock(ResultSet.class);
		Row row = mock(Row.class);

		// When
		when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(ps);
		when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId(), EACH_QUORUM)).thenReturn(
				bsWrapper);
		when(resultSet.all()).thenReturn(Arrays.asList(row));
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(context.executeImmediate(bsWrapper)).thenReturn(resultSet);

		// Then
		Row actual = daoContext.loadProperty(context, pm);

		assertThat(actual).isSameAs(row);
	}

	@Test
	public void should_return_null_when_loading_property() throws Exception {
		// Given
		ResultSet resultSet = mock(ResultSet.class);
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
				.consistencyLevels(Pair.create(ONE, ALL)).build();

		// When
		when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(ps);
		when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId(), EACH_QUORUM)).thenReturn(
				bsWrapper);
		when(resultSet.all()).thenReturn(Lists.<Row> newLinkedList());
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(context.executeImmediate(bsWrapper)).thenReturn(resultSet);

		// Then
		assertThat(daoContext.loadProperty(context, pm)).isNull();
	}

	@Test
	public void should_execute_query() throws Exception {
		// Given
		ResultSet resultSet = mock(ResultSet.class);

		// When
		when(bsWrapper.execute(session)).thenReturn(resultSet);

		// Then
		ResultSet actual = daoContext.execute(bsWrapper);

		assertThat(actual).isSameAs(resultSet);
	}

	// Simple counter
	@Test
	public void should_bind_simple_counter_increment() throws Exception {
		// Given
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

		// When
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
		when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), 2L, EACH_QUORUM))
				.thenReturn(bsWrapper);

		daoContext.bindForSimpleCounterIncrement(context, entityMeta, pm, 2L);

		// Then
		verify(context).pushCounterStatement(bsWrapper);
	}

	@Test
	public void should_increment_simple_counter() throws Exception {
		// Given
		Long counterValue = RandomUtils.nextLong();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name").build();

		// When
		when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
		when(
				binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), counterValue,
						EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.incrementSimpleCounter(context, entityMeta, pm, counterValue, EACH_QUORUM);

		// Then
		verify(context).executeImmediate(bsWrapper);
	}

	@Test
	public void should_decrement_simple_counter() throws Exception {
		// Given
		Long counterValue = RandomUtils.nextLong();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name").build();

		// When
		when(counterQueryMap.get(CQLQueryType.DECR)).thenReturn(ps);
		when(
				binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), counterValue,
						EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.decrementSimpleCounter(context, entityMeta, pm, counterValue, EACH_QUORUM);

		// Then
		verify(context).executeImmediate(bsWrapper);
	}

	@Test
	public void should_get_simple_counter() throws Exception {
		// Given
		ResultSet resultSet = mock(ResultSet.class);
		Row row = mock(Row.class);
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

		// When
		when(counterQueryMap.get(CQLQueryType.SELECT)).thenReturn(ps);
		when(binder.bindForSimpleCounterSelect(ps, entityMeta, pm, entity.getId(), EACH_QUORUM)).thenReturn(bsWrapper);

		when(context.executeImmediate(bsWrapper)).thenReturn(resultSet);
		when(resultSet.all()).thenReturn(Arrays.<Row> asList(row));

		// Then
		Row actual = daoContext.getSimpleCounter(context, pm, EACH_QUORUM);
		assertThat(actual).isSameAs(row);
	}

	@Test
	public void should_bind_simple_counter_delete() throws Exception {
		// Given
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

		// When
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(counterQueryMap.get(CQLQueryType.DELETE)).thenReturn(ps);
		when(binder.bindForSimpleCounterDelete(ps, entityMeta, pm, 11L, EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.bindForSimpleCounterDelete(context, entityMeta, pm, 11L);

		// Then
		verify(context).pushCounterStatement(bsWrapper);
	}

	// Clustered counter
	@Test
	public void should_push_clustered_counter_increment() throws Exception {
		// Given
		PropertyMeta counterMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("count")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

		// When
		when(context.getTtt()).thenReturn(Optional.<Integer> absent());
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.INCR, ps));
		when(binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, entity.getId(), 2L, EACH_QUORUM))
				.thenReturn(bsWrapper);

		daoContext.pushClusteredCounterIncrementStatement(context, entityMeta, counterMeta, 2L);

		// Then
		verify(context).pushCounterStatement(bsWrapper);
	}


	@Test
	public void should_increment_clustered_counter() throws Exception {
		// Given
		Long counterValue = RandomUtils.nextLong();
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.INCR, ps));

		// When
		when(
				binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, entity.getId(), counterValue,
						EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.incrementClusteredCounter(context, entityMeta, counterValue, EACH_QUORUM);

		// Then
		verify(context).executeImmediate(bsWrapper);
	}

	@Test
	public void should_decrement_clustered_counter() throws Exception {
		// Given
		Long counterValue = RandomUtils.nextLong();
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DECR, ps));

		// When
		when(
				binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, entity.getId(), counterValue,
						EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.decrementClusteredCounter(context, entityMeta, counterValue, EACH_QUORUM);

		// Then
		verify(context).executeImmediate(bsWrapper);
	}

	@Test
	public void should_get_clustered_counter() throws Exception {
		// Given
		ResultSet resultSet = mock(ResultSet.class);
		Row row = mock(Row.class);
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.SELECT, ps));

		// When
		when(binder.bindForClusteredCounterSelect(ps, entityMeta, entity.getId(), EACH_QUORUM)).thenReturn(bsWrapper);
		when(context.executeImmediate(bsWrapper)).thenReturn(resultSet);
		when(resultSet.all()).thenReturn(Arrays.<Row> asList(row));

		// Then
		Row actual = daoContext.getClusteredCounter(context, EACH_QUORUM);

		assertThat(actual).isSameAs(row);
	}

	@Test
	public void should_bind_clustered_counter_delete() throws Exception {
		// Given
		PropertyMeta counterMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("count")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DELETE, ps));

		// When
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(binder.bindForClusteredCounterDelete(ps, entityMeta, 11L, EACH_QUORUM)).thenReturn(bsWrapper);

		daoContext.bindForClusteredCounterDelete(context, entityMeta, counterMeta, 11L);

		// Then
		verify(context).pushCounterStatement(bsWrapper);
	}

	@Test
	public void should_bind_clustered_counter_delete_with_runtime_consistency() throws Exception {
		// Given
		PropertyMeta counterMeta = PropertyMetaTestBuilder.valueClass(Long.class).field("count")
				.consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();
		clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DELETE, ps));

		// When
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(LOCAL_QUORUM));
		when(binder.bindForClusteredCounterDelete(ps, entityMeta, 11L, LOCAL_QUORUM)).thenReturn(bsWrapper);

		daoContext.bindForClusteredCounterDelete(context, entityMeta, counterMeta, 11L);

		// Then
		verify(context).pushCounterStatement(bsWrapper);
	}

	@Test
	public void should_prepare_statement() throws Exception {
		RegularStatement statement = new SimpleStatement("query");
		when(session.prepare("query")).thenReturn(ps);

		assertThat(daoContext.prepare(statement)).isSameAs(ps);
	}

	@Test
	public void should_bind_and_execute_prepared_statement() throws Exception {
		ResultSet rs = mock(ResultSet.class);
		when(ps.bind(11L, "a")).thenReturn(bs);
		when(bs.preparedStatement()).thenReturn(ps);
		when(session.execute(bs)).thenReturn(rs);

		assertThat(daoContext.bindAndExecute(ps, 11L, "a")).isSameAs(rs);
	}

	@Test
	public void should_execute_batch() throws Exception {
		// Given
		BatchStatement batch = mock(BatchStatement.class);

		// When
		daoContext.executeBatch(batch);

		// Then
		verify(session).execute(batch);
	}
}
