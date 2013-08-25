package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.Counter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.CascadeType;
import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.Sets;

/**
 * CQLPersisterImplTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPersisterImplTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CQLPersisterImpl persisterImpl = new CQLPersisterImpl();

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private CQLEntityPersister entityPersister;

    @Mock
    private CQLPersistenceContext context;

    @Mock
    private CQLPersistenceContext joinContext;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private EntityMeta joinMeta;

    private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();
    private Set<PropertyMeta> joinMetas = new HashSet<PropertyMeta>();

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp()
    {
        when(context.getEntity()).thenReturn(entity);
        when(context.getPrimaryKey()).thenReturn(entity.getId());
        when(context.getEntityMeta()).thenReturn(entityMeta);

        when(entityMeta.getAllMetas()).thenReturn(allMetas);
        when(entityMeta.getAllMetasExceptIdMeta()).thenReturn(allMetas);
        allMetas.clear();
        joinMetas.clear();
    }

    @Test
    public void should_persist() throws Exception
    {
        persisterImpl.persist(context);

        verify(context).pushInsertStatement();
    }

    @Test
    public void should_persist_clustered_counter() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .accessors()
                .build();
        Counter counter = CounterBuilder.incr();

        when(context.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);
        when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(counter);

        persisterImpl.persistClusteredCounter(context);

        verify(context).pushClusteredCounterIncrementStatement(counterMeta, 1L);
    }

    @Test
    public void should_exception_when_null_value_for_clustered_counter() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .accessors()
                .build();

        when(context.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);
        when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(null);

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Cannot insert clustered counter entity '" + entity
                + "' with null clustered counter value");
        persisterImpl.persistClusteredCounter(context);

    }

    @Test
    public void should_persist_counters() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .accessors()
                .build();

        when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(CounterBuilder.incr(12L));

        persisterImpl.persistCounters(context, Sets.newHashSet(counterMeta));

        verify(context).bindForSimpleCounterIncrement(counterMeta, 12L);
    }

    @Test
    public void should_not_persist_counters_when_no_counter_set() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .accessors()
                .build();

        when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(null);

        persisterImpl.persistCounters(context, Sets.newHashSet(counterMeta));

        verify(context, never()).bindForSimpleCounterIncrement(eq(counterMeta), any(Long.class));
    }

    @Test
    public void should_cascade_persist() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .joinMeta(joinMeta)
                .cascadeType(CascadeType.ALL)
                .build();
        joinMetas.add(joinSimpleMeta);

        UserBean user = new UserBean();
        entity.setUser(user);
        when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(user);

        when(context.createContextForJoin(joinMeta, user)).thenReturn(joinContext);

        persisterImpl.cascadePersist(entityPersister, context, joinMetas);

        verify(entityPersister).persist(joinContext);
    }

    @Test
    public void should_check_for_entity_existence() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .joinMeta(joinMeta)
                .cascadeType(CascadeType.ALL)
                .build();

        joinMetas.add(joinSimpleMeta);
        UserBean user = new UserBean();
        entity.setUser(user);
        when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(user);

        when(context.createContextForJoin(joinMeta, user)).thenReturn(joinContext);
        when(context.addToProcessingList(user)).thenReturn(true);
        when(joinContext.checkForEntityExistence()).thenReturn(true);
        persisterImpl.ensureEntitiesExist(context, joinMetas);

        verify(joinContext).checkForEntityExistence();
    }

    @Test
    public void should_not_check_for_entity_existence_if_join_already_processed() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .joinMeta(joinMeta)
                .cascadeType(CascadeType.ALL)
                .build();

        joinMetas.add(joinSimpleMeta);
        UserBean user = new UserBean();
        entity.setUser(user);
        when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(user);

        when(context.createContextForJoin(joinMeta, user)).thenReturn(joinContext);
        when(context.addToProcessingList(user)).thenReturn(false);

        persisterImpl.ensureEntitiesExist(context, joinMetas);

        verify(joinContext, never()).checkForEntityExistence();
    }

    @Test
    public void should_not_check_for_entity_existence_if_null_join_entity() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .joinMeta(joinMeta)
                .cascadeType(CascadeType.ALL)
                .build();

        joinMetas.add(joinSimpleMeta);
        when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(null);

        persisterImpl.ensureEntitiesExist(context, joinMetas);

        verify(joinContext, never()).checkForEntityExistence();
    }

    @Test
    public void should_remove() throws Exception
    {
        when(entityMeta.isClusteredCounter()).thenReturn(false);
        when(entityMeta.getTableName()).thenReturn("table");
        when(entityMeta.getWriteConsistencyLevel()).thenReturn(EACH_QUORUM);

        persisterImpl.remove(context);

        verify(context).bindForRemoval("table");
    }

    @Test
    public void should_remove_clustered_counter() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .accessors()
                .build();

        when(entityMeta.isClusteredCounter()).thenReturn(true);
        when(entityMeta.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);

        persisterImpl.remove(context);

        verify(context).bindForClusteredCounterRemoval(counterMeta);
    }

    @Test
    public void should_remove_linked_counters() throws Exception
    {
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.COUNTER)
                .consistencyLevels(Pair.create(ALL, EACH_QUORUM))
                .build();

        allMetas.add(counterMeta);

        persisterImpl.removeLinkedCounters(context);

        verify(context).bindForSimpleCounterRemoval(counterMeta);
    }

}
