package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.base.Optional;

/**
 * CQLPersistenceContextFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceContextFactoryTest {

    private CQLPersistenceContextFactory factory;

    @Mock
    private CQLDaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private CQLEntityProxifier proxifier;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private CQLImmediateFlushContext flushContext;

    private Map<Class<?>, EntityMeta> entityMetaMap;

    private EntityMeta meta;

    private PropertyMeta<?, ?> idMeta;

    @Before
    public void setUp() throws Exception
    {
        meta = new EntityMeta();
        idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .build();
        meta.setIdMeta(idMeta);
        meta.setEntityClass(CompleteBean.class);
        entityMetaMap = new HashMap<Class<?>, EntityMeta>();
        entityMetaMap.put(CompleteBean.class, meta);

        factory = new CQLPersistenceContextFactory(daoContext, configContext, entityMetaMap);
        Whitebox.setInternalState(factory, ReflectionInvoker.class, invoker);
    }

    @Test
    public void should_create_new_context_for_entity_with_consistency_and_ttl() throws Exception
    {
        Long primaryKey = RandomUtils.nextLong();
        CompleteBean entity = new CompleteBean(primaryKey);

        when((Class) proxifier.deriveBaseClass(entity)).thenReturn(CompleteBean.class);

        CQLPersistenceContext actual = factory.newContext(entity,
                Optional.fromNullable(EACH_QUORUM),
                Optional.fromNullable(LOCAL_QUORUM),
                Optional.<Integer> fromNullable(95));

        assertThat(actual.getEntity()).isSameAs(entity);
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(actual.getEntityMeta()).isSameAs(meta);
        assertThat(actual.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(actual.getTttO().get()).isEqualTo(95);
    }

    @Test
    public void should_create_new_context_for_entity() throws Exception
    {
        Long primaryKey = RandomUtils.nextLong();
        CompleteBean entity = new CompleteBean(primaryKey);

        when((Class) proxifier.deriveBaseClass(entity)).thenReturn(CompleteBean.class);

        CQLPersistenceContext actual = factory.newContext(entity);

        assertThat(actual.getEntity()).isSameAs(entity);
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(actual.getEntityMeta()).isSameAs(meta);
        assertThat(actual.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(actual.getTttO()).isSameAs(CQLPersistenceContextFactory.NO_TTL);
    }

    @Test
    public void should_create_new_context_with_primary_key() throws Exception
    {
        Object primaryKey = RandomUtils.nextLong();
        CQLPersistenceContext context = factory.newContext(CompleteBean.class, primaryKey,
                Optional.fromNullable(EACH_QUORUM),
                Optional.fromNullable(LOCAL_QUORUM), Optional.fromNullable(98));

        assertThat(context.getEntity()).isNull();
        assertThat(context.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(context.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(context.getEntityMeta()).isSameAs(meta);
        assertThat(context.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(context.getTttO().get()).isEqualTo(98);
    }

    @Test
    public void should_create_new_join_context_with_join_entity() throws Exception
    {
        Long primaryKey = RandomUtils.nextLong();
        CompleteBean entity = new CompleteBean(primaryKey);
        when((Class) proxifier.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
        when(flushContext.duplicateWithoutTtl()).thenReturn(flushContext);
        when(flushContext.getTtlO()).thenReturn(Optional.fromNullable(88));
        CQLPersistenceContext actual = factory.newContextForJoin(entity, flushContext, new HashSet<String>());

        assertThat(actual.getEntity()).isSameAs(entity);
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(actual.getEntityMeta()).isSameAs(meta);
        assertThat(actual.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(actual.getTttO().get()).isEqualTo(88);
    }

    @Test
    public void should_create_new_join_context_with_join_id() throws Exception
    {
        Long primaryKey = RandomUtils.nextLong();
        when(flushContext.duplicateWithoutTtl()).thenReturn(flushContext);
        when(flushContext.getTtlO()).thenReturn(Optional.fromNullable(78));
        CQLPersistenceContext actual = factory.newContextForJoin(CompleteBean.class,
                primaryKey, flushContext, new HashSet<String>());

        assertThat(actual.getEntity()).isNull();
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(actual.getEntityMeta()).isSameAs(meta);
        assertThat(actual.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(actual.getTttO().get()).isEqualTo(78);
    }

    @Test
    public void should_create_new_context_for_slice_query() throws Exception
    {
        Long primaryKey = RandomUtils.nextLong();
        CompleteBean entity = new CompleteBean(primaryKey);
        when(invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, primaryKey)).thenReturn(primaryKey);

        CQLPersistenceContext actual = factory.newContextForSliceQuery(CompleteBean.class, primaryKey, EACH_QUORUM);

        assertThat(actual.getEntity()).isNull();
        assertThat(actual.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.getEntityClass()).isSameAs((Class) CompleteBean.class);
        assertThat(actual.getEntityMeta()).isSameAs(meta);
        assertThat(actual.getIdMeta()).isSameAs((PropertyMeta) idMeta);
        assertThat(actual.getTttO().isPresent()).isFalse();
    }
}
