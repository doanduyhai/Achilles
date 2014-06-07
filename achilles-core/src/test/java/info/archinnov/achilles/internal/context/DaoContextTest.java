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

import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.collect.ImmutableMap.of;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.SELECT_ALL;
import static info.archinnov.achilles.internal.context.DaoContext.RESULTSET_TO_ROW;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.datastax.driver.core.querybuilder.Using;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.StatementGenerator;
import info.archinnov.achilles.internal.statement.cache.CacheManager;
import info.archinnov.achilles.internal.statement.cache.StatementCacheKey;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementBinder;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

@RunWith(MockitoJUnitRunner.class)
public class DaoContextTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private DaoContext daoContext;

    @Mock
    private StatementGenerator statementGenerator;


    @Mock
    private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;

    @Mock
    private Map<Class<?>, PreparedStatement> selectEagerPSs;

    @Mock
    private Map<Class<?>, Map<String, PreparedStatement>> deletePSs;

    @Mock
    private Map<CQLQueryType, PreparedStatement> counterQueryMap;

    private Map<Class<?>, Map<CQLQueryType, Map<String, PreparedStatement>>> clusteredCounterQueryMap = new HashMap<>();

    @Mock
    private Session session;

    @Mock
    private PreparedStatementBinder binder;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private ExecutorService executorService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private AsyncUtils asyncUtils;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PersistenceContext.DaoFacade context;

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

    @Mock
    private DirtyCheckChangeSet changeSet;

    @Mock
    private ConsistencyOverrider overrider;

    @Mock
    private SliceQueryProperties<CompleteBean> sliceQueryProperties;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet;

    @Mock
    private ListenableFuture<Row> futureRow;

    @Captor
    private ArgumentCaptor<Using> usingCaptor;

    @Captor
    private ArgumentCaptor<RegularStatementWrapper> statementWrapperCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp() {
        daoContext.binder = binder;
        daoContext.cacheManager = cacheManager;
        daoContext.dynamicPSCache = dynamicPSCache;
        daoContext.selectPSs = selectEagerPSs;
        daoContext.deletePSs = deletePSs;
        daoContext.counterQueryMap = counterQueryMap;
        daoContext.clusteredCounterQueryMap = clusteredCounterQueryMap;
        daoContext.session = session;
        daoContext.statementGenerator = statementGenerator;
        daoContext.overrider = overrider;
        daoContext.asyncUtils = asyncUtils;
        daoContext.executorService = executorService;

        clusteredCounterQueryMap.clear();
        entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(entityMeta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(entityMeta.config().getConsistencyLevels()).thenReturn(Pair.create(ONE, EACH_QUORUM));
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntity()).thenReturn(entity);
        when(context.getPrimaryKey()).thenReturn(entity.getId());
        when(context.getOptions().isIfNotExists()).thenReturn(false);

        selectEagerPSs.clear();
        deletePSs.clear();
    }

    @Test
    public void should_push_insert() throws Exception {
        // Given
//        entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));
        List<PropertyMeta> pms = new ArrayList<>();

        when(cacheManager.getCacheForEntityInsert(session, dynamicPSCache, context, pms)).thenReturn(ps);
        when(binder.bindForInsert(context, ps, pms)).thenReturn(bsWrapper);
        when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel>fromNullable(null));

        // When
        daoContext.pushInsertStatement(context, pms);

        // Then
        verify(context).pushStatement(bsWrapper);
    }

    @Test
    public void should_push_update() throws Exception {
        // Given
        List<PropertyMeta> pms = new ArrayList<>();

        when(cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms)).thenReturn(ps);
        when(binder.bindForUpdate(context, ps, pms)).thenReturn(bsWrapper);

        // When
        daoContext.pushUpdateStatement(context, pms);

        // Then
        verify(context).pushStatement(bsWrapper);
    }

    @Test
    public void should_push_collection_and_map_update() throws Exception {
        // Given
        PropertyMeta setMeta = PropertyMetaTestBuilder.valueClass(String.class).propertyName("followers").build();

        when(changeSet.getChangeType()).thenReturn(ADD_TO_SET);
        when(changeSet.getPropertyMeta()).thenReturn(setMeta);
        when(cacheManager.getCacheForCollectionAndMapOperation(session, dynamicPSCache, context, setMeta, changeSet)).thenReturn(ps);
        when(binder.bindForCollectionAndMapUpdate(context, ps, changeSet)).thenReturn(bsWrapper);

        // When
        daoContext.pushCollectionAndMapUpdateStatement(context, changeSet);

        // Then
        verify(context).pushStatement(bsWrapper);
    }

    @Test
    public void should_push_list_set_at_index_update() throws Exception {
        // Given
        final Where where = update("test").where();
        Object[] boundValues = new Object[] { "whatever" };
        Pair<Where, Object[]> pair = Pair.create(where, boundValues);

        final Optional<CASResultListener> casResultListener = Optional.absent();

        when(changeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);

        when(overrider.getWriteLevel(context)).thenReturn(EACH_QUORUM);
        when(statementGenerator.generateCollectionAndMapUpdateOperation(context, changeSet)).thenReturn(pair);
        when(context.getCASResultListener()).thenReturn(casResultListener);
        when(context.getSerialConsistencyLevel()).thenReturn(fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL));

        // When
        daoContext.pushCollectionAndMapUpdateStatement(context, changeSet);

        // Then
        verify(context).pushStatement(statementWrapperCaptor.capture());
        final RegularStatementWrapper statementWrapper = statementWrapperCaptor.getValue();
        assertThat(statementWrapper.getValues()).contains(boundValues);
        assertThat(statementWrapper.getStatement().getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
        assertThat(statementWrapper.getStatement().getSerialConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
        assertThat(where.getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_push_list_remove_at_index_update() throws Exception {
        // Given
        final Where where = update("test").where();
        Object[] boundValues = new Object[] { "whatever" };
        Pair<Where, Object[]> pair = Pair.create(where, boundValues);

        final Optional<CASResultListener> casResultListener = Optional.absent();

        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);

        when(overrider.getWriteLevel(context)).thenReturn(EACH_QUORUM);
        when(statementGenerator.generateCollectionAndMapUpdateOperation(context, changeSet)).thenReturn(pair);
        when(context.getCASResultListener()).thenReturn(casResultListener);

        when(statementGenerator.generateCollectionAndMapUpdateOperation(context, changeSet)).thenReturn(pair);

        // When
        daoContext.pushCollectionAndMapUpdateStatement(context, changeSet);

        // Then
        verify(context).pushStatement(statementWrapperCaptor.capture());
        assertThat(statementWrapperCaptor.getValue().getValues()).contains(boundValues);

        assertThat(where.getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_bind_for_deletion() throws Exception {
        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(deletePSs.get(CompleteBean.class)).thenReturn(of("table", ps));
        when(overrider.getWriteLevel(context)).thenReturn(EACH_QUORUM);
        when(binder.bindStatementWithOnlyPKInWhereClause(context, ps, false, EACH_QUORUM)).thenReturn(bsWrapper);

        daoContext.bindForDeletion(context, entityMeta, "table");

        verify(context).pushStatement(bsWrapper);
    }

    @Test
    public void should_exception_when_deletion_ps_not_found_for_a_table() throws Exception {
        when(deletePSs.get(CompleteBean.class)).thenReturn(of("some_table", ps));
        when(context.getConsistencyLevel()).thenReturn(fromNullable(EACH_QUORUM));
        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot find prepared statement for deletion for table 'table'");

        daoContext.bindForDeletion(context, entityMeta, "table");
    }

    @Test
    public void should_eager_load_entity() throws Exception {
        // Given
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);


        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).build();

        when(entityMeta.structure().hasOnlyStaticColumns()).thenReturn(false);
        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(entityMeta.getAllMetasExceptId()).thenReturn(asList(pm));
        when(selectEagerPSs.get(CompleteBean.class)).thenReturn(ps);
        when(overrider.getReadLevel(context)).thenReturn(LOCAL_QUORUM);
        when(binder.bindStatementWithOnlyPKInWhereClause(context, ps, false, LOCAL_QUORUM)).thenReturn(bsWrapper);
        when(context.executeImmediate(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);

        // When
        final ListenableFuture<Row> actual = daoContext.loadEntity(context);

        // Then
        assertThat(actual).isSameAs(futureRow);
    }

    @Test
    public void should_load_property() throws Exception {
        // Given
        PropertyMeta pm = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        Row row = mock(Row.class);

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(ps);
        when(pm.structure().isStaticColumn()).thenReturn(true);
        when(overrider.getReadLevel(context)).thenReturn(EACH_QUORUM);
        when(binder.bindStatementWithOnlyPKInWhereClause(context, ps, true, EACH_QUORUM)).thenReturn(bsWrapper);
        when(context.executeImmediate(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);
        when(asyncUtils.buildInterruptible(futureRow).getImmediately()).thenReturn(row);

        // When
        Row actual = daoContext.loadProperty(context, pm);

        // Then

        assertThat(actual).isSameAs(row);
    }

    @Test
    public void should_execute_query() throws Exception {
        // Given
        when(bsWrapper.executeAsync(session, executorService)).thenReturn(futureResultSet);

        // When
        final ListenableFuture<ResultSet> actual = daoContext.execute(bsWrapper);

        // Then
        assertThat(actual).isSameAs(futureResultSet);
    }

    // Simple counter
    @Test
    public void should_bind_simple_counter_increment() throws Exception {
        // Given
        PropertyMeta pm = mock(PropertyMeta.class);

        // When
        when(counterQueryMap.get(INCR)).thenReturn(ps);
        when(overrider.getWriteLevel(context, pm)).thenReturn(EACH_QUORUM);
        when(binder.bindForSimpleCounterIncrementDecrement(context, ps, pm, 2L, EACH_QUORUM)).thenReturn(bsWrapper);

        daoContext.bindForSimpleCounterIncrement(context, pm, 2L);

        // Then
        verify(context).pushCounterStatement(bsWrapper);
    }

    @Test
    public void should_increment_simple_counter() throws Exception {
        // Given
        Long counterValue = RandomUtils.nextLong(0, Long.MAX_VALUE);
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).propertyName("name").build();

        // When
        when(counterQueryMap.get(INCR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(context, ps, pm, counterValue, EACH_QUORUM)).thenReturn(bsWrapper);

        daoContext.incrementSimpleCounter(context, pm, counterValue, EACH_QUORUM);

        // Then
        verify(context).executeImmediate(bsWrapper);
    }

    @Test
    public void should_decrement_simple_counter() throws Exception {
        // Given
        Long counterValue = RandomUtils.nextLong(0, Long.MAX_VALUE);
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).propertyName("name").build();

        // When
        when(counterQueryMap.get(CQLQueryType.DECR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(context, ps, pm, counterValue, EACH_QUORUM)).thenReturn(bsWrapper);

        daoContext.decrementSimpleCounter(context, pm, counterValue, EACH_QUORUM);

        // Then
        verify(context).executeImmediate(bsWrapper);
    }

    @Test
    public void should_get_simple_counter() throws Exception {
        // Given
        Row row = mock(Row.class);
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).propertyName("name").consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

        when(counterQueryMap.get(CQLQueryType.SELECT)).thenReturn(ps);
        when(binder.bindForSimpleCounterSelect(context, ps, pm, EACH_QUORUM)).thenReturn(bsWrapper);
        when(context.executeImmediate(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);
        when(asyncUtils.buildInterruptible(futureRow).getImmediately()).thenReturn(row);
        when(row.isNull(ACHILLES_COUNTER_VALUE)).thenReturn(false);
        when(row.getLong(ACHILLES_COUNTER_VALUE)).thenReturn(11L);

        // When
        Long actual = daoContext.getSimpleCounter(context, pm, EACH_QUORUM);

        // Then
        assertThat(actual).isSameAs(11L);
    }

    @Test
    public void should_bind_simple_counter_delete() throws Exception {
        // Given
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).propertyName("name").consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();

        // When
        when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel>fromNullable(null));
        when(counterQueryMap.get(CQLQueryType.DELETE)).thenReturn(ps);
        when(binder.bindForSimpleCounterDelete(context, ps, pm)).thenReturn(bsWrapper);

        daoContext.bindForSimpleCounterDelete(context, pm);

        // Then
        verify(context).pushCounterStatement(bsWrapper);
    }

    // Clustered counter
    @Test
    public void should_push_clustered_counter_increment() throws Exception {
        // Given
        PropertyMeta counterMeta = PropertyMetaTestBuilder.valueClass(Long.class).propertyName("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).staticColumn(false).build();

        // When
        when(context.getTtl()).thenReturn(Optional.<Integer>absent());
        when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel>fromNullable(null));
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.<CQLQueryType, Map<String, PreparedStatement>>of(INCR, of("count", ps)));
        when(binder.bindForClusteredCounterIncrementDecrement(context, ps, counterMeta, 2L)).thenReturn(bsWrapper);

        daoContext.pushClusteredCounterIncrementStatement(context, counterMeta, 2L);

        // Then
        verify(context).pushCounterStatement(bsWrapper);
    }

    @Test
    public void should_get_clustered_counter() throws Exception {
        // Given
        // Given
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.<CQLQueryType, Map<String, PreparedStatement>>of(SELECT, of(SELECT_ALL.name(), ps)));

        when(overrider.getReadLevel(context)).thenReturn(EACH_QUORUM);
        when(binder.bindForClusteredCounterSelect(context, ps, false, EACH_QUORUM)).thenReturn(bsWrapper);
        when(context.executeImmediate(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);

        // When
        ListenableFuture<Row> actual = daoContext.getClusteredCounter(context);

        // Then
        assertThat(actual).isSameAs(futureRow);
    }

    @Test
    public void should_get_clustered_counter_column() throws Exception {
        // Given
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(counterMeta.getCQL3ColumnName()).thenReturn("counter");
        when(counterMeta.structure().isStaticColumn()).thenReturn(true);
        Row row = mock(Row.class);

        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.<CQLQueryType, Map<String, PreparedStatement>>of(SELECT, of("counter", ps)));
        when(overrider.getReadLevel(context, counterMeta)).thenReturn(EACH_QUORUM);
        when(binder.bindForClusteredCounterSelect(context, ps, true, EACH_QUORUM)).thenReturn(bsWrapper);
        when(context.executeImmediate(bsWrapper)).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROW, executorService)).thenReturn(futureRow);
        when(asyncUtils.buildInterruptible(futureRow).getImmediately()).thenReturn(row);

        when(row.isNull("counter")).thenReturn(false);
        when(row.getLong("counter")).thenReturn(11L);

        // When
        Long actual = daoContext.getClusteredCounterColumn(context, counterMeta);

        // Then
        assertThat(actual).isEqualTo(11L);
    }

    @Test
    public void should_bind_clustered_counter_delete() throws Exception {
        // Given
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.<CQLQueryType, Map<String, PreparedStatement>>of(DELETE, of(DELETE_ALL.name(), ps)));

        // When
        when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel>fromNullable(null));
        when(binder.bindForClusteredCounterDelete(context, ps)).thenReturn(bsWrapper);

        daoContext.bindForClusteredCounterDelete(context);

        // Then
        verify(context).pushCounterStatement(bsWrapper);
    }

    @Test
    public void should_bind_clustered_counter_delete_with_runtime_consistency() throws Exception {
        // Given
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.<CQLQueryType, Map<String, PreparedStatement>>of(DELETE, of(DELETE_ALL.name(), ps)));

        // When
        when(context.getConsistencyLevel()).thenReturn(fromNullable(LOCAL_QUORUM));
        when(binder.bindForClusteredCounterDelete(context, ps)).thenReturn(bsWrapper);

        daoContext.bindForClusteredCounterDelete(context);

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
    public void should_bind_for_slice_query_select() throws Exception {
        //Given
        final Object[] boundValues = { 10 };
        when(cacheManager.getCacheForSliceSelectAndIterator(session, dynamicPSCache, sliceQueryProperties)).thenReturn(ps);
        when(sliceQueryProperties.getEntityClass()).thenReturn(CompleteBean.class);
        when(sliceQueryProperties.getBoundValues()).thenReturn(boundValues);
        when(sliceQueryProperties.getConsistencyLevelOr(EACH_QUORUM)).thenReturn(LOCAL_QUORUM);
        when(ps.bind(boundValues)).thenReturn(bs);

        //When
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQuerySelect(sliceQueryProperties, EACH_QUORUM);

        //Then
        assertThat(bsWrapper.getStatement()).isSameAs(bs);
        assertThat(bsWrapper.getValues()).isSameAs(boundValues);

        verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
        verify(sliceQueryProperties).setFetchSizeToStatement(bs);
    }

    @Test
    public void should_bind_for_slice_query_delete() throws Exception {
        //Given
        final Object[] boundValues = { 10 };
        when(cacheManager.getCacheForSliceDelete(session, dynamicPSCache, sliceQueryProperties)).thenReturn(ps);
        when(sliceQueryProperties.getEntityClass()).thenReturn(CompleteBean.class);
        when(sliceQueryProperties.getBoundValues()).thenReturn(boundValues);
        when(sliceQueryProperties.getConsistencyLevelOr(EACH_QUORUM)).thenReturn(LOCAL_QUORUM);
        when(ps.bind(boundValues)).thenReturn(bs);

        //When
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQueryDelete(sliceQueryProperties, EACH_QUORUM);

        //Then
        assertThat(bsWrapper.getStatement()).isSameAs(bs);
        assertThat(bsWrapper.getValues()).isSameAs(boundValues);

        verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
        verify(sliceQueryProperties).setFetchSizeToStatement(bs);
    }
}
