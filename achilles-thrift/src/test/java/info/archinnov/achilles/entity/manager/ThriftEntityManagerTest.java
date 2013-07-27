package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftSliceQueryExecutor;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.base.Optional;

/**
 * ThriftEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private ThriftEntityManager em;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock
    private Map<String, ThriftGenericEntityDao> entityDaosMap;

    @Mock
    private EntityMeta entityMeta;

    private PropertyMeta<Void, Long> idMeta;

    @Mock
    private ThriftGenericEntityDao entityDao;

    @Mock
    private ThriftEntityProxifier proxifier;

    @Mock
    private ThriftDaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private ThriftPersistenceContextFactory contextFactory;

    private ThriftSliceQueryExecutor queryExecutor;

    private ThriftCompoundKeyValidator compoundKeyValidator;

    private Optional<ConsistencyLevel> noConsistency = Optional.<ConsistencyLevel> absent();
    private Optional<Integer> noTtl = Optional.<Integer> absent();

    private Long primaryKey = 1165446L;
    private CompleteBean entity = CompleteBeanTestBuilder
            .builder()
            .id(primaryKey)
            .name("name")
            .buid();

    @Before
    public void setUp() throws Exception
    {
        em.setContextFactory(contextFactory);
        em.setQueryExecutor(queryExecutor);
        em.setCompoundKeyValidator(compoundKeyValidator);
        em.setProxifier(proxifier);
        em.setEntityMetaMap(entityMetaMap);
        em.setConfigContext(configContext);
        em.setThriftDaoContext(daoContext);
    }

    @Test
    public void should_init_persistence_context_with_class_and_primary_key() throws Exception
    {
        ThriftPersistenceContext context = mock(ThriftPersistenceContext.class);
        when(contextFactory.newContext(CompleteBean.class, entity.getId(), noConsistency, noConsistency, noTtl))
                .thenReturn(context);

        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(daoContext.findEntityDao("table")).thenReturn(entityDao);

        ThriftPersistenceContext actual = em.initPersistenceContext(CompleteBean.class,
                entity.getId(), noConsistency, noConsistency, noTtl);

        assertThat(actual).isSameAs(context);
    }

    @Test
    public void should_init_persistence_context_with_entity() throws Exception
    {
        ThriftPersistenceContext context = mock(ThriftPersistenceContext.class);
        when(contextFactory.newContext(entity, noConsistency, noConsistency, noTtl)).thenReturn(context);

        ThriftPersistenceContext actual = em.initPersistenceContext(entity, noConsistency,
                noConsistency, noTtl);

        assertThat(actual).isSameAs(context);

    }

    @Test
    public void should_create_slice_query_builder() throws Exception
    {
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);

        SliceQueryBuilder<ThriftPersistenceContext, CompleteBean> builder = em.sliceQuery(CompleteBean.class);

        assertThat(builder).isNotNull();
        assertThat(Whitebox.getInternalState(builder, "sliceQueryExecutor")).isSameAs(queryExecutor);
        assertThat(Whitebox.getInternalState(builder, "meta")).isSameAs(entityMeta);
        assertThat(Whitebox.getInternalState(builder, "entityClass")).isEqualTo(CompleteBean.class);
    }
}
