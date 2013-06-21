package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * ThriftPersisterImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftPersisterImplTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftPersisterImpl thriftPersister;

    @Mock
    private ThriftEntityPersister persister;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private EntityProxifier<ThriftPersistenceContext> proxifier;

    @Mock
    private ThriftGenericEntityDao entityDao;

    @Mock
    private ThriftGenericWideRowDao columnFamilyDao;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private ThriftCompositeFactory thriftCompositeFactory;

    @Mock
    private ThriftCounterDao thriftCounterDao;

    @Mock
    private Mutator<Object> mutator;

    @Mock
    private Mutator<Object> cfMutator;

    @Mock
    private Mutator<Object> counterMutator;

    @Mock
    private ThriftConsistencyLevelPolicy policy;

    @Mock
    private Map<String, ThriftGenericEntityDao> entityDaosMap;

    @Mock
    private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap;

    @Mock
    private ThriftImmediateFlushContext thriftImmediateFlushContext;

    private Optional<Integer> ttlO = Optional.<Integer> absent();

    @Captor
    ArgumentCaptor<Composite> compositeCaptor;

    private ObjectMapper objectMapper = new ObjectMapper();

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private ThriftPersistenceContext context;

    @Before
    public void setUp()
    {
        context = ThriftPersistenceContextTestBuilder
                .context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
                .entity(entity)
                .thriftImmediateFlushContext(thriftImmediateFlushContext)
                .entityDao(entityDao)
                .columnFamilyDao(columnFamilyDao)
                .columnFamilyDaosMap(columnFamilyDaosMap)
                .entityDaosMap(entityDaosMap)
                .build();
        when(entityMeta.getTableName()).thenReturn("cf");
        when(thriftImmediateFlushContext.getEntityMutator("cf")).thenReturn(mutator);
        when(thriftImmediateFlushContext.getTtlO()).thenReturn(ttlO);
    }

    @Test
    public void should_batch_simple_property() throws Exception
    {

        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .build();

        Composite comp = new Composite();
        when(thriftCompositeFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(comp);

        when(invoker.getValueFromField(entity, propertyMeta.getGetter())).thenReturn("testValue");

        thriftPersister.batchPersistSimpleProperty(context, propertyMeta);

        verify(entityDao).insertColumnBatch(entity.getId(), comp, "testValue", ttlO, mutator);

    }

    @Test
    public void should_batch_list_property() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("friends")
                .accessors()
                .build();

        Composite comp1 = new Composite();
        Composite comp2 = new Composite();
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 0)).thenReturn(
                comp1);
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(
                comp2);

        thriftPersister.batchPersistList(Arrays.asList("foo", "bar"), context, propertyMeta);

        InOrder inOrder = inOrder(entityDao);
        inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "foo", ttlO, mutator);
        inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "bar", ttlO, mutator);

    }

    @Test
    public void should_batch_set_property() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("followers")
                .accessors()
                .build();

        Composite comp1 = new Composite();
        Composite comp2 = new Composite();
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, "John".hashCode()))
                .thenReturn(comp1);
        when(
                thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                        "Helen".hashCode())).thenReturn(comp2);

        Set<String> followers = ImmutableSet.of("John", "Helen");
        thriftPersister.batchPersistSet(followers, context, propertyMeta);

        InOrder inOrder = inOrder(entityDao);
        inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "John", ttlO, mutator);
        inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "Helen", ttlO, mutator);

    }

    @Test
    public void should_batch_map_property() throws Exception
    {
        PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Integer.class, String.class)
                .field("preferences")
                .type(MAP)
                .accessors()
                .build();

        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "FR");
        map.put(2, "Paris");
        map.put(3, "75014");

        Composite comp1 = new Composite();
        Composite comp2 = new Composite();
        Composite comp3 = new Composite();
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(
                comp1);
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 2)).thenReturn(
                comp2);
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 3)).thenReturn(
                comp3);

        thriftPersister.batchPersistMap(map, context, propertyMeta);

        ArgumentCaptor<String> keyValueHolderCaptor = ArgumentCaptor.forClass(String.class);

        verify(entityDao, times(3)).insertColumnBatch(eq(entity.getId()), any(Composite.class),
                keyValueHolderCaptor.capture(), eq(ttlO), eq(mutator));

        assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);

        List<String> keyValues = keyValueHolderCaptor.getAllValues();

        KeyValue<Integer, String> holder1 = readKeyValue(keyValues.get(0));
        KeyValue<Integer, String> holder2 = readKeyValue(keyValues.get(1));
        KeyValue<Integer, String> holder3 = readKeyValue(keyValues.get(2));

        assertThat(holder1.getKey()).isEqualTo(1);
        assertThat(holder1.getValue()).isEqualTo("FR");

        assertThat(holder2.getKey()).isEqualTo(2);
        assertThat(holder2.getValue()).isEqualTo("Paris");

        assertThat(holder3.getKey()).isEqualTo(3);
        assertThat(holder3.getValue()).isEqualTo("75014");
    }

    @Test
    public void should_batch_persist_join_entity() throws Exception
    {
        Long joinId = 154654L;
        PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
                .of(UserBean.class, Void.class, Long.class)
                .field("userId")
                .type(SIMPLE)
                .accessors()
                .build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .accessors()
                .build();

        UserBean user = new UserBean();
        user.setUserId(joinId);

        when(invoker.getPrimaryKey(user, joinIdMeta)).thenReturn(joinId);
        Composite comp = new Composite();
        when(thriftCompositeFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(comp);

        when(proxifier.unproxy(user)).thenReturn(user);

        thriftPersister.batchPersistJoinEntity(context, propertyMeta, user, persister);

        verify(entityDao).insertColumnBatch(entity.getId(), comp, joinId.toString(), ttlO, mutator);

        ArgumentCaptor<ThriftPersistenceContext> contextCaptor = ArgumentCaptor
                .forClass(ThriftPersistenceContext.class);
        verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user),
                eq(propertyMeta.getJoinProperties()));
        assertThat(contextCaptor.getValue().getEntity()).isSameAs(user);
    }

    @Test
    public void should_batch_persist_join_collection() throws Exception
    {
        Long joinId1 = 54351L, joinId2 = 4653L;
        PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
                .of(UserBean.class, Void.class, Long.class)
                .field("userId")
                .type(SIMPLE)
                .accessors()
                .build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .joinMeta(joinMeta)
                .build();

        UserBean user1 = new UserBean(), user2 = new UserBean();
        user1.setUserId(joinId1);
        user2.setUserId(joinId2);

        Composite comp1 = new Composite();
        Composite comp2 = new Composite();
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 0)).thenReturn(
                comp1);
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(
                comp2);
        when(invoker.getValueFromField(user1, joinIdMeta.getGetter())).thenReturn(joinId1);
        when(invoker.getValueFromField(user2, joinIdMeta.getGetter())).thenReturn(joinId2);

        when(proxifier.unproxy(user1)).thenReturn(user1);
        when(proxifier.unproxy(user2)).thenReturn(user2);

        thriftPersister.batchPersistJoinCollection(context, propertyMeta,
                Arrays.asList(user1, user2), persister);

        verify(entityDao).insertColumnBatch(entity.getId(), comp1, joinId1.toString(), ttlO,
                mutator);
        verify(entityDao).insertColumnBatch(entity.getId(), comp2, joinId2.toString(), ttlO,
                mutator);

        ArgumentCaptor<ThriftPersistenceContext> contextCaptor = ArgumentCaptor
                .forClass(ThriftPersistenceContext.class);
        verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user1),
                eq(propertyMeta.getJoinProperties()));
        verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user2),
                eq(propertyMeta.getJoinProperties()));

        List<ThriftPersistenceContext> contextes = contextCaptor.getAllValues();

        assertThat(contextes.get(0).getEntity()).isSameAs(user1);
        assertThat(contextes.get(1).getEntity()).isSameAs(user2);

    }

    @Test
    public void should_batch_persist_join_map() throws Exception
    {
        Long joinId1 = 54351L, joinId2 = 4653L;
        PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
                .of(UserBean.class, Void.class, Long.class)
                .field("userId")
                .type(SIMPLE)
                .accessors()
                .build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Integer.class, UserBean.class)
                .joinMeta(joinMeta)
                .build();

        UserBean user1 = new UserBean(), user2 = new UserBean();
        user1.setUserId(joinId1);
        user2.setUserId(joinId2);

        Map<Integer, UserBean> joinMap = ImmutableMap.of(1, user1, 2, user2);
        KeyValue<Integer, String> kv1 = new KeyValue<Integer, String>(1, joinId1.toString());
        KeyValue<Integer, String> kv2 = new KeyValue<Integer, String>(2, joinId2.toString());

        Composite comp1 = new Composite();
        Composite comp2 = new Composite();

        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(
                comp1);
        when(thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta, 2)).thenReturn(
                comp2);
        when(invoker.getValueFromField(user1, joinIdMeta.getGetter())).thenReturn(joinId1);
        when(invoker.getValueFromField(user2, joinIdMeta.getGetter())).thenReturn(joinId2);

        when(proxifier.unproxy(user1)).thenReturn(user1);
        when(proxifier.unproxy(user2)).thenReturn(user2);

        thriftPersister.batchPersistJoinMap(context, propertyMeta, joinMap, persister);

        verify(entityDao).insertColumnBatch(entity.getId(), comp1, writeString(kv1), ttlO, mutator);
        verify(entityDao).insertColumnBatch(entity.getId(), comp2, writeString(kv2), ttlO, mutator);

        ArgumentCaptor<ThriftPersistenceContext> contextCaptor = ArgumentCaptor
                .forClass(ThriftPersistenceContext.class);

        verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user1),
                eq(propertyMeta.getJoinProperties()));
        verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user2),
                eq(propertyMeta.getJoinProperties()));

        List<ThriftPersistenceContext> contextes = contextCaptor.getAllValues();

        assertThat(contextes.get(0).getEntity()).isSameAs(user1);
        assertThat(contextes.get(1).getEntity()).isSameAs(user2);
    }

    @Test
    public void should_remove_wide_row() throws Exception
    {
        when(entityMeta.isWideRow()).thenReturn(true);
        when((Mutator) thriftImmediateFlushContext.getWideRowMutator("cf")).thenReturn(mutator);
        thriftPersister.remove(context);
        verify(columnFamilyDao).removeRowBatch(entity.getId(), mutator);
    }

    @Test
    public void should_remove_entity_having_external_wide_map() throws Exception
    {
        when(entityMeta.isWideRow()).thenReturn(false);
        PropertyMeta<UUID, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(UUID.class, String.class)
                .field("geoPositions")
                .type(PropertyType.WIDE_MAP)
                .externalTable("external_cf")
                .idClass(Long.class)
                .accessors()
                .build();

        Map<String, PropertyMeta<UUID, String>> propertyMetas = ImmutableMap.of("geoPositions",
                propertyMeta);
        when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
        when(columnFamilyDaosMap.get("external_cf")).thenReturn(columnFamilyDao);
        when((Mutator) thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(
                cfMutator);

        thriftPersister.remove(context);
        verify(entityDao).removeRowBatch(entity.getId(), mutator);
        verify(columnFamilyDao).removeRowBatch(entity.getId(), cfMutator);
    }

    @Test
    public void should_remove_entity_having_simple_counter() throws Exception
    {
        String fqcn = CompleteBean.class.getCanonicalName();

        PropertyMeta<Void, Long> counterIdMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .build();

        PropertyMeta<Void, Counter> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Counter.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .accessors()
                .counterIdMeta(counterIdMeta)
                .fqcn(fqcn)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ALL))
                .build();

        when(entityMeta.isWideRow()).thenReturn(false);
        Map<String, PropertyMeta<Void, Counter>> propertyMetas = ImmutableMap.of("geoPositions",
                propertyMeta);
        when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

        Composite keyComp = new Composite();
        Composite comp = new Composite();
        when(thriftCompositeFactory.createKeyForCounter(fqcn, entity.getId(), counterIdMeta))
                .thenReturn(keyComp);
        when(thriftCompositeFactory.createForBatchInsertSingleCounter(propertyMeta)).thenReturn(
                comp);
        when(thriftImmediateFlushContext.getCounterMutator()).thenReturn(counterMutator);

        thriftPersister.remove(context);

        verify(thriftCounterDao).removeCounterBatch(keyComp, comp, counterMutator);

    }

    @SuppressWarnings("rawtypes")
    @Test
    public void should_remove_entity_having_widemap_counter() throws Exception
    {
        String fqcn = CompleteBean.class.getCanonicalName();

        PropertyMeta<Void, Long> counterIdMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class)
                //
                .field("id")
                //
                .accessors()
                //
                .build();
        PropertyMeta<String, Counter> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(String.class, Counter.class)
                //
                .field("popularTopics")
                //
                .type(PropertyType.COUNTER_WIDE_MAP)
                //
                .accessors()
                //
                .counterIdMeta(counterIdMeta)
                //
                .fqcn(fqcn)
                //
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ALL))
                //
                .build();

        when(entityMeta.isWideRow()).thenReturn(false);
        Map<String, PropertyMeta<String, Counter>> propertyMetas = ImmutableMap.of("geoPositions",
                propertyMeta);
        when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

        Composite keyComp = new Composite();
        when(thriftCompositeFactory.createKeyForCounter(fqcn, entity.getId(), counterIdMeta))
                .thenReturn(keyComp);
        when(thriftImmediateFlushContext.getCounterMutator()).thenReturn(counterMutator);

        thriftPersister.remove(context);

        verify(thriftCounterDao).removeCounterRowBatch(keyComp, counterMutator);

    }

    @Test
    public void should_batch_remove_property() throws Exception
    {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                //
                .field("name")
                //
                .type(PropertyType.SIMPLE)
                //
                .accessors()
                //
                .build();

        Composite start = new Composite(), end = new Composite();
        when(thriftCompositeFactory.createBaseForQuery(propertyMeta, ComponentEquality.EQUAL))
                .thenReturn(start);
        when(
                thriftCompositeFactory.createBaseForQuery(propertyMeta,
                        ComponentEquality.GREATER_THAN_EQUAL)).thenReturn(end);

        thriftPersister.removePropertyBatch(context, propertyMeta);

        verify(entityDao).removeColumnRangeBatch(entity.getId(), start, end, mutator);
    }

    @SuppressWarnings("unchecked")
    private KeyValue<Integer, String> readKeyValue(String value) throws Exception
    {
        return objectMapper.readValue(value, KeyValue.class);
    }

    private String writeString(Object value) throws Exception
    {
        return objectMapper.writeValueAsString(value);
    }
}
