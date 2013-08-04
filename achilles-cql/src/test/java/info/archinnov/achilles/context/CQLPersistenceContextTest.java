package info.archinnov.achilles.context;

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
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

/**
 * CQLPersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceContextTest
{
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

    private EntityMeta meta;

    private PropertyMeta<?, ?> idMeta;

    private Long primaryKey = RandomUtils.nextLong();

    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

    @Before
    public void setUp() throws Exception
    {
        idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

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
        Whitebox.setInternalState(context, CQLAbstractFlushContext.class, flushContext);
    }

    @Test
    public void should_create_new_persistence_context_for_join_entity() throws Exception
    {

        CQLPersistenceContext joinContext = context.createContextForJoin(meta, entity);

        assertThat(joinContext.getEntity()).isSameAs(entity);
        assertThat((Class) joinContext.getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(joinContext.getEntityMeta()).isSameAs(meta);
        assertThat(joinContext.getPrimaryKey()).isEqualTo(primaryKey);
    }

    @Test
    public void should_create_new_persistence_context_with_id_and_type() throws Exception
    {
        CQLPersistenceContext joinContext = context.createContextForJoin(CompleteBean.class, meta,
                primaryKey);

        assertThat((Class) joinContext.getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(joinContext.getEntityMeta()).isSameAs(meta);
        assertThat(joinContext.getPrimaryKey()).isEqualTo(primaryKey);
    }

    @Test
    public void should_duplicate_for_new_entity() throws Exception
    {
        CompleteBean entity = new CompleteBean();
        entity.setId(primaryKey);
        CQLPersistenceContext joinContext = context.duplicate(entity);

        assertThat(joinContext.getEntity()).isSameAs(entity);
        assertThat(joinContext.getPrimaryKey()).isSameAs(primaryKey);
    }

    @Test
    public void should_check_for_entity_existence() throws Exception
    {
        when(daoContext.checkForEntityExistence(context)).thenReturn(true);

        assertThat(context.checkForEntityExistence()).isTrue();
    }

    @Test
    public void should_eager_load_entity() throws Exception
    {
        Row row = mock(Row.class);
        when(daoContext.eagerLoadEntity(context)).thenReturn(row);

        assertThat(context.eagerLoadEntity()).isSameAs(row);
    }

    @Test
    public void should_load_property() throws Exception
    {
        Row row = mock(Row.class);
        when(daoContext.loadProperty(context, idMeta)).thenReturn(row);

        assertThat(context.loadProperty(idMeta)).isSameAs(row);
    }

    @Test
    public void should_bind_for_insert() throws Exception
    {
        context.pushInsertStatement();

        verify(daoContext).pushInsertStatement(context);
    }

    @Test
    public void should_bind_for_update() throws Exception
    {
        List<PropertyMeta<?, ?>> pms = Arrays.asList();
        context.pushUpdateStatement(pms);

        verify(daoContext).pushUpdateStatement(context, pms);
    }

    @Test
    public void should_bind_for_removal() throws Exception
    {
        context.bindForRemoval("table");

        verify(daoContext).bindForRemoval(context, "table");
    }

    @Test
    public void should_bind_and_execute() throws Exception
    {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(daoContext.bindAndExecute(ps, 11L, "a")).thenReturn(rs);
        ResultSet actual = context.bindAndExecute(ps, 11L, "a");

        assertThat(actual).isSameAs(rs);
    }

    // Simple counter
    @Test
    public void should_bind_for_simple_counter_increment() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.bindForSimpleCounterIncrement(counterMeta, 11L);

        verify(daoContext).bindForSimpleCounterIncrement(context, meta, counterMeta, 11L);
    }

    @Test
    public void should_increment_simple_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.incrementSimpleCounter(counterMeta, 11L, LOCAL_QUORUM);

        verify(daoContext).incrementSimpleCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
    }

    @Test
    public void should_decrement_simple_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.decrementSimpleCounter(counterMeta, 11L, LOCAL_QUORUM);

        verify(daoContext).decrementSimpleCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
    }

    @Test
    public void should_get_simple_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        Row row = mock(Row.class);
        when(daoContext.getSimpleCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(row);
        when(row.getLong(CQL_COUNTER_VALUE)).thenReturn(11L);
        Long counterValue = context.getSimpleCounter(counterMeta, LOCAL_QUORUM);

        assertThat(counterValue).isEqualTo(11L);
    }

    @Test
    public void should_return_null_when_no_simple_counter_value() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        when(daoContext.getSimpleCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(null);

        assertThat(context.getSimpleCounter(counterMeta, LOCAL_QUORUM)).isNull();
    }

    @Test
    public void should_bind_for_simple_counter_removal() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.bindForSimpleCounterRemoval(counterMeta);

        verify(daoContext).bindForSimpleCounterDelete(context, meta, counterMeta, entity.getId());
    }

    // Clustered counter
    @Test
    public void should_bind_for_clustered_counter_increment() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.pushClusteredCounterIncrementStatement(counterMeta, 11L);

        verify(daoContext).pushClusteredCounterIncrementStatement(context, meta, counterMeta, 11L);
    }

    @Test
    public void should_increment_clustered_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.incrementClusteredCounter(counterMeta, 11L, LOCAL_QUORUM);

        verify(daoContext).incrementClusteredCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
    }

    @Test
    public void should_decrement_clustered_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.decrementClusteredCounter(counterMeta, 11L, LOCAL_QUORUM);

        verify(daoContext).decrementClusteredCounter(context, meta, counterMeta, 11L, LOCAL_QUORUM);
    }

    @Test
    public void should_get_clustered_counter() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();
        counterMeta.setPropertyName("count");

        Row row = mock(Row.class);
        when(daoContext.getClusteredCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(row);
        when(row.getLong("count")).thenReturn(11L);
        Long counterValue = context.getClusteredCounter(counterMeta, LOCAL_QUORUM);

        assertThat(counterValue).isEqualTo(11L);
    }

    @Test
    public void should_return_null_when_no_clustered_counter_value() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        when(daoContext.getClusteredCounter(context, counterMeta, LOCAL_QUORUM)).thenReturn(null);

        assertThat(context.getClusteredCounter(counterMeta, LOCAL_QUORUM)).isNull();
    }

    @Test
    public void should_bind_for_clustered_counter_removal() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();

        context.bindForClusteredCounterRemoval(counterMeta);

        verify(daoContext).bindForClusteredCounterDelete(context, meta, counterMeta, entity.getId());
    }

    @Test
    public void should_push_bound_statement() throws Exception
    {
        BoundStatement bs = mock(BoundStatement.class);

        context.pushBoundStatement(bs, EACH_QUORUM);

        verify(flushContext).pushBoundStatement(bs, EACH_QUORUM);
    }

    @Test
    public void should_execute_immediate_with_consistency() throws Exception
    {
        BoundStatement bs = mock(BoundStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(flushContext.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);

        ResultSet actual = context.executeImmediateWithConsistency(bs, EACH_QUORUM);

        assertThat(actual).isSameAs(resultSet);
    }

    @Test
    public void should_persist() throws Exception
    {
        context.persist();
        verify(persister).persist(context);
        verify(flushContext).flush();
    }

    @Test
    public void should_merge() throws Exception
    {
        when(merger.merge(context, entity)).thenReturn(entity);

        CompleteBean merged = context.merge(entity);

        assertThat(merged).isSameAs(entity);
        verify(flushContext).flush();
    }

    @Test
    public void should_remove() throws Exception
    {
        context.remove();
        verify(persister).remove(context);
        verify(flushContext).flush();
    }

    @Test
    public void should_find() throws Exception
    {
        when(loader.load(context, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxy(entity, context)).thenReturn(entity);

        CompleteBean found = context.find(CompleteBean.class);

        assertThat(found).isSameAs(entity);
    }

    @Test
    public void should_return_null_when_not_found() throws Exception
    {
        when(loader.load(context, CompleteBean.class)).thenReturn(null);

        CompleteBean found = context.find(CompleteBean.class);

        assertThat(found).isNull();
        verifyZeroInteractions(proxifier);
    }

    @Test
    public void should_get_reference() throws Exception
    {
        when(loader.load(context, CompleteBean.class)).thenReturn(entity);
        when(proxifier.buildProxy(entity, context)).thenReturn(entity);

        CompleteBean found = context.getReference(CompleteBean.class);

        assertThat(context.isLoadEagerFields()).isFalse();
        assertThat(found).isSameAs(entity);
    }

    @Test
    public void should_refresh() throws Exception
    {
        context.refresh();
        verify(refresher).refresh(context);
    }

    @Test
    public void should_initialize() throws Exception
    {
        EntityInterceptor<CQLPersistenceContext, CompleteBean> interceptor = mock(EntityInterceptor.class);

        when(proxifier.getInterceptor(entity)).thenReturn(interceptor);

        CompleteBean actual = context.initialize(entity);

        assertThat(actual).isSameAs(entity);

        verify(proxifier).ensureProxy(entity);
        verify(initializer).initializeEntity(entity, meta, interceptor);
    }
}
