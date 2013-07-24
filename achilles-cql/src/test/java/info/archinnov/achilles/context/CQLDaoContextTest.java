package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.CQLPreparedStatementBinder;
import info.archinnov.achilles.statement.cache.CacheManager;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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
    private Map<Class<?>, PreparedStatement> insertPSs;

    @Mock
    private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;

    @Mock
    private Map<Class<?>, PreparedStatement> selectEagerPSs;

    @Mock
    private Map<Class<?>, Map<String, PreparedStatement>> removePSs;

    @Mock
    private Map<CQLQueryType, PreparedStatement> counterQueryMap;

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
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

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
        Whitebox.setInternalState(daoContext, Session.class, session);

        entityMeta = new EntityMeta();
        entityMeta.setConsistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE,
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
    public void should_bind_for_insert() throws Exception
    {
        when(insertPSs.get(CompleteBean.class)).thenReturn(ps);
        when(binder.bindForInsert(ps, entityMeta, entity)).thenReturn(bs);

        daoContext.bindForInsert(context);
        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_for_update() throws Exception
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
        when(cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms))
                .thenReturn(ps);
        when(binder.bindForUpdate(ps, entityMeta, pms, entity)).thenReturn(bs);

        daoContext.bindForUpdate(context, pms);
        verify(context).pushBoundStatement(bs, EACH_QUORUM);
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

        daoContext.bindForRemoval(context, "table", EACH_QUORUM);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_exception_when_removal_ps_not_found_for_a_table() throws Exception
    {
        when(removePSs.get(CompleteBean.class)).thenReturn(ImmutableMap.of("some_table", ps));

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot find prepared statement for deletion for table 'table'");

        daoContext.bindForRemoval(context, "table", EACH_QUORUM);
    }

    @Test
    public void should_eager_load_entity() throws Exception
    {
        entityMeta.setConsistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM));
        when(selectEagerPSs.get(CompleteBean.class)).thenReturn(ps);
        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.all()).thenReturn(Arrays.asList(row));
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);

        Row actual = daoContext.eagerLoadEntity(context);

        assertThat(actual).isSameAs(row);

    }

    @Test
    public void should_load_property() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(
                ps);

        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.all()).thenReturn(Arrays.asList(row));
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);

        Row actual = daoContext.loadProperty(context, pm);

        assertThat(actual).isSameAs(row);
    }

    @Test
    public void should_return_null_when_loading_property() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm)).thenReturn(
                ps);

        when(binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity.getId()))
                .thenReturn(bs);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.all()).thenReturn(Lists.<Row> newLinkedList());
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);

        assertThat(daoContext.loadProperty(context, pm)).isNull();
    }

    @Test
    public void should_execute_query() throws Exception
    {
        ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bs)).thenReturn(resultSet);

        ResultSet actual = daoContext.execute(bs);

        assertThat(actual).isSameAs(resultSet);
    }

    @Test
    public void should_bind_counter_increment() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(counterQueryMap.get(CQLQueryType.INCR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, 11L, 2L))
                .thenReturn(bs);

        daoContext.bindForSimpleCounterIncrement(context, entityMeta, pm, 11L, 2L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_counter_decrement() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(counterQueryMap.get(CQLQueryType.DECR)).thenReturn(ps);
        when(binder.bindForSimpleCounterIncrementDecrement(ps, entityMeta, pm, 11L, 2L))
                .thenReturn(bs);

        daoContext.bindForSimpleCounterDecrement(context, entityMeta, pm, 11L, 2L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_counter_delete() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(counterQueryMap.get(CQLQueryType.DELETE)).thenReturn(ps);
        when(binder.bindForSimpleCounterDelete(ps, entityMeta, pm, 11L)).thenReturn(bs);

        daoContext.bindForSimpleCounterDelete(context, entityMeta, pm, 11L);

        verify(context).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_bind_counter_select() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();

        when(counterQueryMap.get(CQLQueryType.SELECT)).thenReturn(ps);
        when(binder.bindForSimpleCounterSelect(ps, entityMeta, pm, 11L)).thenReturn(bs);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(context.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);
        when(resultSet.all()).thenReturn(Arrays.<Row> asList(row));

        Row actual = daoContext.bindForSimpleCounterSelect(context, entityMeta, pm, 11L);

        assertThat(actual).isSameAs(row);
    }
}
