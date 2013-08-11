package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryValidator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;

/**
 * CqlEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityManagerTest
{
    private CQLEntityManager manager;

    @Mock
    private CQLEntityManagerFactory factory;

    @Mock
    private CQLEntityProxifier proxifier;

    @Mock
    private CQLDaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private CQLPersistenceContextFactory contextFactory;

    @Mock
    private AchillesConsistencyLevelPolicy policy;

    @Mock
    private CQLTypedQueryValidator typedQueryValidator;

    private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

    private EntityMeta meta;

    private PropertyMeta idMeta;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private Optional<ConsistencyLevel> noConsistency = Optional.<ConsistencyLevel> absent();
    private Optional<Integer> noTtl = Optional.<Integer> absent();

    @Before
    public void setUp() throws Exception
    {
        idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setEntityClass(CompleteBean.class);
        meta.setPropertyMetas(new HashMap<String, PropertyMeta>());

        when(configContext.getConsistencyPolicy()).thenReturn(policy);
        when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);

        manager = new CQLEntityManager(entityMetaMap, contextFactory, daoContext, configContext);
        Whitebox.setInternalState(manager, CQLEntityProxifier.class, proxifier);
        Whitebox.setInternalState(manager, CQLTypedQueryValidator.class, typedQueryValidator);

        manager.setEntityMetaMap(entityMetaMap);
        entityMetaMap.put(CompleteBean.class, meta);
    }

    @Test
    public void should_init_persistence_context_with_entity() throws Exception
    {
        CQLPersistenceContext context = mock(CQLPersistenceContext.class);
        when(contextFactory.newContext(entity, noConsistency, noConsistency, noTtl)).thenReturn(context);

        CQLPersistenceContext actual = manager.initPersistenceContext(entity, noConsistency,
                noConsistency, noTtl);

        assertThat(actual).isSameAs(context);

    }

    @Test
    public void should_init_persistence_context_with_type_and_id() throws Exception
    {
        CQLPersistenceContext context = mock(CQLPersistenceContext.class);
        when(contextFactory.newContext(CompleteBean.class, entity.getId(), noConsistency, noConsistency, noTtl))
                .thenReturn(
                        context);

        CQLPersistenceContext actual = manager.initPersistenceContext(CompleteBean.class,
                entity.getId(), noConsistency, noConsistency, noTtl);

        assertThat(actual).isSameAs(context);
    }

    @Test
    public void should_return_slice_query_builder() throws Exception
    {
        SliceQueryBuilder<CQLPersistenceContext, CompleteBean> builder = manager.sliceQuery(CompleteBean.class);

        assertThat(builder).isNotNull();

        assertThat(Whitebox.getInternalState(builder, SliceQueryExecutor.class)).isNotNull();
        assertThat(Whitebox.getInternalState(builder, CompoundKeyValidator.class)).isNotNull();
        assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
        assertThat(Whitebox.getInternalState(builder, Class.class)).isSameAs(CompleteBean.class);
    }

    @Test
    public void should_return_native_query_builder() throws Exception
    {
        CQLNativeQueryBuilder builder = manager.nativeQuery("queryString");

        assertThat(builder).isNotNull();

        assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("queryString");
    }

    @Test
    public void should_return_typed_query_builder() throws Exception
    {

        CQLTypedQueryBuilder<CompleteBean> builder = manager.typedQuery(CompleteBean.class, "queryString");

        assertThat(builder).isNotNull();

        verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "queryString", meta);

        assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
        assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
    }

    @Test
    public void should_return_raw_typed_query_builder() throws Exception
    {

        CQLTypedQueryBuilder<CompleteBean> builder = manager.rawTypedQuery(CompleteBean.class, "queryString");

        assertThat(builder).isNotNull();

        verify(typedQueryValidator).validateRawTypedQuery(CompleteBean.class, "queryString", meta);

        assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
        assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
    }

    @Test
    public void should_get_native_session() throws Exception
    {
        Session session = mock(Session.class);
        when(daoContext.getSession()).thenReturn(session);

        Session actual = manager.getNativeSession();

        assertThat(actual).isSameAs(session);
    }
}
