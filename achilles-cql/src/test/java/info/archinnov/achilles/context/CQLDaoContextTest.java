package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.statement.cache.CacheManager;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.statement.prepared.CQLPreparedStatementBinder;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;
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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Using;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * CQLDaoContextTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CQLDaoContextTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CQLDaoContext daoContext;

    @Mock
    private CQLStatementGenerator statementGenerator;

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
    private CQLPreparedStatementBinder binder;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private CQLPersistenceContext context;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private Insert insert;

    @Mock
    private Insert.Options insertOptions;

    @Mock
    private Assignments update;

    @Mock
    private Update.Options updateOptions;

    @Mock
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

    @Captor
    ArgumentCaptor<Using> usingCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp()
    {
        Whitebox.setInternalState(daoContext, CQLPreparedStatementBinder.class, binder);
        Whitebox.setInternalState(daoContext, CacheManager.class, cacheManager);
        Whitebox.setInternalState(daoContext, "insertPSs", insertPSs);
        Whitebox.setInternalState(daoContext, Cache.class, dynamicPSCache);
        Whitebox.setInternalState(daoContext, "selectEagerPSs", selectEagerPSs);
        Whitebox.setInternalState(daoContext, "removePSs", removePSs);
        Whitebox.setInternalState(daoContext, "counterQueryMap", counterQueryMap);
        Whitebox.setInternalState(daoContext, "clusteredCounterQueryMap", clusteredCounterQueryMap);
        Whitebox.setInternalState(daoContext, Session.class, session);
        Whitebox.setInternalState(daoContext, CQLStatementGenerator.class, statementGenerator);
        clusteredCounterQueryMap.clear();
        entityMeta = new EntityMeta();
        entityMeta.setEntityClass(CompleteBean.class);
        entityMeta.setConsistencyLevels(Pair.create(ONE,
                EACH_QUORUM));

        when(context.getEntityMeta()).thenReturn(entityMeta);
        when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntity()).thenReturn(entity);
        when(context.getPrimaryKey()).thenReturn(entity.getId());

        insertPSs.clear();
        selectEagerPSs.clear();
        removePSs.clear();
    }

    @Test
    public void should_push_insert_without_ttl() throws Exception
    {
        when(context.getTttO()).thenReturn(Optional.<Integer> absent());
        entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));
        when(insertPSs.get(CompleteBean.class)).thenReturn(ps);
        when(binder.bindForInsert(ps, entityMeta, entity)).thenReturn(bs);
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        daoContext.pushInsertStatement(context);
        verify(context).pushBoundStatement(bs, ALL);
    }

    @Test
    public void should_push_insert_with_ttl() throws Exception
    {
        int ttl = 115;
        when(context.getTttO()).thenReturn(Optional.<Integer> fromNullable(ttl));
        when(context.getEntity()).thenReturn(entity);
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));

        entityMeta.setConsistencyLevels(Pair.create(ONE, ALL));

        when(statementGenerator.generateInsert(entity, entityMeta)).thenReturn(insert);
        when(insert.using(usingCaptor.capture())).thenReturn(insertOptions);

        daoContext.pushInsertStatement(context);
        verify(context).pushStatement(insertOptions, ALL);
        assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(new Long(ttl));
    }

    @Test
    public void should_push_update_without_ttl() throws Exception
    {
        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("age")
                .build();

        List<PropertyMeta<?, ?>> pms = Arrays.asList(nameMeta, ageMeta);
        when(context.getTttO()).thenReturn(Optional.<Integer> absent());
        when(cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms))
                .thenReturn(ps);
        when(binder.bindForUpdate(ps, entityMeta, pms, entity)).thenReturn(bs);
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));

        daoContext.pushUpdateStatement(context, pms);
        verify(context).pushBoundStatement(bs, EACH_QUORUM);

    }

    @Test
    public void should_push_update_with_ttl() throws Exception
    {
        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("age")
                .build();

        List<PropertyMeta<?, ?>> pms = Arrays.asList(nameMeta, ageMeta);

        int ttl = 15465;
        when(context.getTttO()).thenReturn(Optional.<Integer> fromNullable(ttl));
        when(context.getEntity()).thenReturn(entity);
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));

        when(statementGenerator.generateUpdateFields(entity, entityMeta, pms)).thenReturn(update);
        when(update.using(usingCaptor.capture())).thenReturn(updateOptions);

        daoContext.pushUpdateStatement(context, pms);
        verify(context).pushStatement(updateOptions, EACH_QUORUM);
        assertThat(Whitebox.getInternalState(usingCaptor.getValue(), "value")).isEqualTo(new Long(ttl));
    }

    @Test
    public void should_check_for_entity_existence() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("id")
                .build();
        entityMeta.setIdMeta(idMeta);
        entityMeta.setConsistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM));

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, idMeta))
                .thenReturn(ps);
        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        ResultSet resultSet = mock(ResultSet.class);
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(resultSet.all()).thenReturn(Arrays.asList(mock(Row.class)));

        boolean actual = daoContext.checkForEntityExistence(context);

        assertThat(actual).isTrue();
    }

    @Test
    public void should_bind_for_removal() throws Exception
    {
        when(removePSs.get(CompleteBean.class)).thenReturn(ImmutableMap.of("table", ps));
        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));

        daoContext.bindForRemoval(context, "table");

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_exception_when_removal_ps_not_found_for_a_table() throws Exception
    {
        when(removePSs.get(CompleteBean.class)).thenReturn(ImmutableMap.of("some_table", ps));
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot find prepared statement for deletion for table 'table'");

        daoContext.bindForRemoval(context, "table");
    }

    @Test
    public void should_eager_load_entity() throws Exception
    {
        entityMeta.setConsistencyLevels(Pair.create(LOCAL_QUORUM, LOCAL_QUORUM));
        when(selectEagerPSs.get(CompleteBean.class)).thenReturn(ps);
        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.all()).thenReturn(Arrays.asList(row));
        when(context.executeImmediateWithConsistency(bs, LOCAL_QUORUM)).thenReturn(resultSet);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));

        Row actual = daoContext.eagerLoadEntity(context);

        assertThat(actual).isSameAs(row);

    }

    @Test
    public void should_load_property() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(ONE, ALL))
                .build();

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(
                ps);

        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.all()).thenReturn(Arrays.asList(row));
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        Row actual = daoContext.loadProperty(context, pm);

        assertThat(actual).isSameAs(row);
    }

    @Test
    public void should_return_null_when_loading_property() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(ONE, ALL))
                .build();

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(
                ps);

        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.all()).thenReturn(Lists.<Row> newLinkedList());
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        assertThat(daoContext.loadProperty(context, pm)).isNull();
    }

    @Test
    public void should_execute_query() throws Exception
    {
        ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bs)).thenReturn(resultSet);
        when(bs.preparedStatement()).thenReturn(ps);

        ResultSet actual = daoContext.execute(bs);

        assertThat(actual).isSameAs(resultSet);
    }

    // Simple counter
    @Test
    public void should_bind_simple_counter_increment() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), 2L))
                .thenReturn(bs);

        daoContext.bindForSimpleCounterIncrement(context, entityMeta, pm, 2L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_simple_counter_increment_with_runtime_consistency() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(LOCAL_QUORUM));
        when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), 2L))
                .thenReturn(bs);

        daoContext.bindForSimpleCounterIncrement(context, entityMeta, pm, 2L);

        verify(context).pushBoundStatement(bs, LOCAL_QUORUM);
    }

    @Test
    public void should_increment_simple_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .build();
        when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), counterValue))
                .thenReturn(bs);

        daoContext.incrementSimpleCounter(context, entityMeta, pm, counterValue, EACH_QUORUM);

        verify(context).executeImmediateWithConsistency(bs, EACH_QUORUM);
    }

    @Test
    public void should_decrement_simple_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .build();
        when(counterQueryMap.get(CQLQueryType.DECR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, entity.getId(), counterValue))
                .thenReturn(bs);

        daoContext.decrementSimpleCounter(context, entityMeta, pm, counterValue, EACH_QUORUM);

        verify(context).executeImmediateWithConsistency(bs, EACH_QUORUM);
    }

    @Test
    public void should_get_simple_counter() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(counterQueryMap.get(CQLQueryType.SELECT)).thenReturn(ps);
        when(binder.bindForSimpleCounterSelect(ps, entityMeta, pm, entity.getId())).thenReturn(bs);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(resultSet.all()).thenReturn(Arrays.<Row> asList(row));

        Row actual = daoContext.getSimpleCounter(context, pm, EACH_QUORUM);

        assertThat(actual).isSameAs(row);
    }

    @Test
    public void should_bind_simple_counter_delete() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        when(counterQueryMap.get(CQLQueryType.DELETE)).thenReturn(ps);
        when(binder.bindForSimpleCounterDelete(ps, entityMeta, pm, 11L)).thenReturn(bs);

        daoContext.bindForSimpleCounterDelete(context, entityMeta, pm, 11L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_simple_counter_delete_with_runtime_consistency() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(LOCAL_QUORUM));
        when(counterQueryMap.get(CQLQueryType.DELETE)).thenReturn(ps);
        when(binder.bindForSimpleCounterDelete(ps, entityMeta, pm, 11L)).thenReturn(bs);

        daoContext.bindForSimpleCounterDelete(context, entityMeta, pm, 11L);

        verify(context).pushBoundStatement(bs, LOCAL_QUORUM);
    }

    //Clustered counter
    @Test
    public void should_push_clustered_counter_increment() throws Exception
    {
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(context.getTttO()).thenReturn(Optional.<Integer> absent());
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.INCR, ps));
        when(binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, counterMeta, entity.getId(), 2L))
                .thenReturn(bs);

        daoContext.pushClusteredCounterIncrementStatement(context, entityMeta, counterMeta, 2L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_push_clustered_counter_increment_with_runtime_consistency() throws Exception
    {
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(context.getTttO()).thenReturn(Optional.<Integer> absent());
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(LOCAL_QUORUM));
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.INCR, ps));
        when(binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, counterMeta, entity.getId(), 2L))
                .thenReturn(bs);

        daoContext.pushClusteredCounterIncrementStatement(context, entityMeta, counterMeta, 2L);

        verify(context).pushBoundStatement(bs, LOCAL_QUORUM);
    }

    @Test
    public void should_increment_clustered_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .build();
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.INCR, ps));
        when(
                binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, counterMeta, entity.getId(),
                        counterValue))
                .thenReturn(bs);

        daoContext.incrementClusteredCounter(context, entityMeta, counterMeta, counterValue, EACH_QUORUM);

        verify(context).executeImmediateWithConsistency(bs, EACH_QUORUM);
    }

    @Test
    public void should_decrement_clustered_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .build();
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DECR, ps));
        when(
                binder.bindForClusteredCounterIncrementDecrement(ps, entityMeta, counterMeta, entity.getId(),
                        counterValue))
                .thenReturn(bs);

        daoContext.decrementClusteredCounter(context, entityMeta, counterMeta, counterValue, EACH_QUORUM);

        verify(context).executeImmediateWithConsistency(bs, EACH_QUORUM);
    }

    @Test
    public void should_get_clustered_counter() throws Exception
    {
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.SELECT, ps));
        when(binder.bindForClusteredCounterSelect(ps, entityMeta, counterMeta, entity.getId())).thenReturn(bs);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(resultSet.all()).thenReturn(Arrays.<Row> asList(row));

        Row actual = daoContext.getClusteredCounter(context, counterMeta, EACH_QUORUM);

        assertThat(actual).isSameAs(row);
    }

    @Test
    public void should_bind_clustered_counter_delete() throws Exception
    {
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DELETE, ps));
        when(binder.bindForClusteredCounterDelete(ps, entityMeta, counterMeta, 11L)).thenReturn(bs);

        daoContext.bindForClusteredCounterDelete(context, entityMeta, counterMeta, 11L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_clustered_counter_delete_with_runtime_consistency() throws Exception
    {
        PropertyMeta<?, ?> counterMeta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .field("count")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(LOCAL_QUORUM));
        clusteredCounterQueryMap.put(CompleteBean.class, ImmutableMap.of(CQLQueryType.DELETE, ps));
        when(binder.bindForClusteredCounterDelete(ps, entityMeta, counterMeta, 11L)).thenReturn(bs);

        daoContext.bindForClusteredCounterDelete(context, entityMeta, counterMeta, 11L);

        verify(context).pushBoundStatement(bs, LOCAL_QUORUM);
    }

    @Test
    public void should_prepare_statement() throws Exception
    {
        Statement statement = new SimpleStatement("query");
        when(session.prepare("query")).thenReturn(ps);

        assertThat(daoContext.prepare(statement)).isSameAs(ps);
    }

    @Test
    public void should_bind_and_execute_prepared_statement() throws Exception
    {
        ResultSet rs = mock(ResultSet.class);
        when(ps.bind(11L, "a")).thenReturn(bs);
        when(bs.preparedStatement()).thenReturn(ps);
        when(session.execute(bs)).thenReturn(rs);

        assertThat(daoContext.bindAndExecute(ps, 11L, "a")).isSameAs(rs);
    }
}
