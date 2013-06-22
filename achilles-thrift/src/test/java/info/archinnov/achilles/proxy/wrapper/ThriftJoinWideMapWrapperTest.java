package info.archinnov.achilles.proxy.wrapper;

import static javax.persistence.CascadeType.*;
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
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.ThriftPropertyHelper;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
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
import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;
import com.google.common.base.Optional;

/**
 * ThriftJoinWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinWideMapWrapperTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftJoinWideMapWrapper<Integer, CompleteBean> wrapper;

    @Mock
    private ThriftGenericWideRowDao dao;

    @Mock
    private ThriftGenericEntityDao joinDao;

    @Mock
    private PropertyMeta<Integer, CompleteBean> propertyMeta;

    @Mock
    private ThriftCompositeFactory thriftCompositeFactory;

    @Mock
    private ThriftEntityPersister persister;

    @Mock
    private ThriftEntityLoader loader;

    @Mock
    private ThriftEntityProxifier proxifier;

    @Mock
    private ThriftPropertyHelper thriftPropertyHelper;

    @Mock
    private ThriftKeyValueFactory thriftKeyValueFactory;

    @Mock
    private ThriftIteratorFactory thriftIteratorFactory;

    @Mock
    private ThriftEntityInterceptor<Long> interceptor;

    @Mock
    private Mutator<Object> mutator;

    @Mock
    private Mutator<Object> joinMutator;

    @Mock
    private ThriftCounterDao thriftCounterDao;

    @Mock
    private ThriftConsistencyLevelPolicy policy;

    @Mock
    private ThriftGenericEntityDao entityDao;

    @Mock
    private ThriftImmediateFlushContext thriftImmediateFlushContext;

    @Captor
    private ArgumentCaptor<Optional<Integer>> ttlOCaptor;

    private EntityMeta entityMeta;

    private EntityMeta joinMeta;

    private ThriftPersistenceContext context;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private Long id = 7425L;

    private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception
    {
        wrapper.setId(id);

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        entityMeta = new EntityMeta();
        entityMeta.setIdMeta(idMeta);

        joinMeta = new EntityMeta();
        joinMeta.setIdMeta(idMeta);
        joinMeta.setTableName("join_cf");

        entityDaosMap.clear();
        context = ThriftPersistenceContextTestBuilder //
                .context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
                .entity(entity)
                .entityDaosMap(entityDaosMap)
                .thriftImmediateFlushContext(thriftImmediateFlushContext)
                .build();
        wrapper.setContext(context);
        when(propertyMeta.getExternalTableName()).thenReturn("external_cf");
        when((Class<Long>) propertyMeta.getIdClass()).thenReturn(Long.class);
    }

    @Test
    public void should_get_value() throws Exception
    {
        int key = 4567;
        Composite comp = new Composite();

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(propertyMeta.joinMeta()).thenReturn(entityMeta);
        when(propertyMeta.getValueClass()).thenReturn(CompleteBean.class);

        when(dao.getValue(id, comp)).thenReturn(entity.getId());
        when(loader.load(any(ThriftPersistenceContext.class), eq(CompleteBean.class))).thenReturn(
                entity);
        when(proxifier.buildProxy(eq(entity), any(ThriftPersistenceContext.class))).thenReturn(
                entity);

        CompleteBean actual = wrapper.get(key);

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void should_get_null_value() throws Exception
    {
        int key = 4567;
        Composite comp = new Composite();

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(propertyMeta.joinMeta()).thenReturn(entityMeta);
        when(propertyMeta.getValueClass()).thenReturn(CompleteBean.class);

        when(dao.getValue(id, comp)).thenReturn(entity.getId());
        when(loader.load(any(ThriftPersistenceContext.class), eq(CompleteBean.class))).thenReturn(
                null);
        when(proxifier.buildProxy(eq(null), any(ThriftPersistenceContext.class))).thenReturn(null);

        assertThat(wrapper.get(key)).isNull();

        when(dao.getValue(id, comp)).thenReturn(null);
        assertThat(wrapper.get(key)).isNull();

    }

    @Test
    public void should_insert_value_and_entity_when_insertable() throws Exception
    {

        JoinProperties joinProperties = prepareJoinProperties();
        joinProperties.addCascadeType(PERSIST);

        Integer key = 4567;
        Composite comp = new Composite();

        when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
        when(propertyMeta.joinMeta()).thenReturn(entityMeta);
        when(
                persister.cascadePersistOrEnsureExists(any(ThriftPersistenceContext.class),
                        eq(entity), eq(joinProperties))).thenReturn(entity.getId());

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(joinDao.buildMutator()).thenReturn(joinMutator);
        when(thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(mutator);
        wrapper.insert(key, entity);

        verify(dao).setValueBatch(id, comp, entity.getId(), Optional.<Integer> absent(), mutator);
        verify(thriftImmediateFlushContext).flush();
    }

    @Test
    public void should_exception_when_trying_to_persist_with_null_key() throws Exception
    {
        exception.expect(AchillesException.class);
        exception.expectMessage("Key should be provided to insert data into WideMap");
        wrapper.insert(null, null);
    }

    @Test
    public void should_exception_when_trying_to_persist_null_value() throws Exception
    {
        exception.expect(AchillesException.class);
        exception.expectMessage("Value should be provided to insert data into WideMap");
        wrapper.insert(4567, null);
    }

    @Test
    public void should_insert_value_and_entity_with_ttl() throws Exception
    {
        JoinProperties joinProperties = prepareJoinProperties();
        joinProperties.addCascadeType(ALL);

        int key = 4567;
        Composite comp = new Composite();

        when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
        when(propertyMeta.joinMeta()).thenReturn(entityMeta);

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(
                persister.cascadePersistOrEnsureExists(any(ThriftPersistenceContext.class),
                        eq(entity), eq(joinProperties))).thenReturn(entity.getId());

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(joinDao.buildMutator()).thenReturn(joinMutator);
        when(thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(mutator);

        wrapper.insert(key, entity, 150);

        verify(dao).setValueBatch(eq(id), eq(comp), eq(entity.getId()), ttlOCaptor.capture(),
                eq(mutator));
        verify(thriftImmediateFlushContext).flush();
        assertThat(ttlOCaptor.getValue().get()).isEqualTo(150);
    }

    @Test
    public void should_find_keyvalue_range() throws Exception
    {
        int start = 7, end = 5, count = 10;

        Composite startComp = new Composite(), endComp = new Composite();

        when(
                thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                        BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
                .thenReturn(new Composite[]
                {
                        startComp,
                        endComp
                });
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        when(
                dao.findRawColumnsRange(id, startComp, endComp, count,
                        OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
        List<KeyValue<Integer, CompleteBean>> values = mock(List.class);
        when(thriftKeyValueFactory.createJoinKeyValueList(context, propertyMeta, hColumns))
                .thenReturn(values);

        List<KeyValue<Integer, CompleteBean>> expected = wrapper.find(start, end, count,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        verify(thriftPropertyHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING,
                false);
        assertThat(expected).isSameAs(values);
    }

    @SuppressWarnings(
    {
            "unchecked",
            "rawtypes"
    })
    @Test
    public void should_find_values_range() throws Exception
    {
        int start = 7, end = 5, count = 10;

        Composite startComp = new Composite(), endComp = new Composite();

        when(
                thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                        BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
                .thenReturn(new Composite[]
                {
                        startComp,
                        endComp
                });
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        when(
                dao.findRawColumnsRange(id, startComp, endComp, count,
                        OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
        List<CompleteBean> values = mock(List.class);
        when(thriftKeyValueFactory.createJoinValueList(context, propertyMeta, hColumns))
                .thenReturn(values);

        List<CompleteBean> expected = wrapper.findValues(start, end, count,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        verify(thriftPropertyHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING,
                false);
        assertThat(expected).isSameAs(values);
    }

    @SuppressWarnings(
    {
            "unchecked",
            "rawtypes"
    })
    @Test
    public void should_find_keys_range() throws Exception
    {
        int start = 7, end = 5, count = 10;

        Composite startComp = new Composite(), endComp = new Composite();

        when(
                thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                        BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
                .thenReturn(new Composite[]
                {
                        startComp,
                        endComp
                });
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        when(
                dao.findRawColumnsRange(id, startComp, endComp, count,
                        OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
        List<Integer> values = mock(List.class);
        when(thriftKeyValueFactory.createKeyList(propertyMeta, hColumns)).thenReturn(values);

        List<Integer> expected = wrapper.findKeys(start, end, count,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        verify(thriftPropertyHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING,
                false);
        assertThat(expected).isSameAs(values);
    }

    @Test
    public void should_get_iterator() throws Exception
    {
        int start = 7, end = 5, count = 10;
        Composite startComp = new Composite(), endComp = new Composite();

        when(
                thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                        BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
                .thenReturn(new Composite[]
                {
                        startComp,
                        endComp
                });

        ThriftJoinSliceIterator<Long, Integer, CompleteBean> iterator = mock(ThriftJoinSliceIterator.class);

        when(propertyMeta.joinMeta()).thenReturn(joinMeta);
        entityDaosMap.put("join_cf", joinDao);
        when(
                dao.getJoinColumnsIterator(joinDao, propertyMeta, id, startComp, endComp,
                        OrderingMode.DESCENDING.isReverse(), count)).thenReturn(iterator);

        KeyValueIterator<Integer, CompleteBean> keyValueIterator = mock(KeyValueIterator.class);
        when(thriftIteratorFactory.createJoinKeyValueIterator(context, iterator, propertyMeta))
                .thenReturn(keyValueIterator);

        KeyValueIterator<Integer, CompleteBean> expected = wrapper.iterator(start, end, count,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        assertThat(expected).isSameAs(keyValueIterator);
    }

    @Test
    public void should_remove() throws Exception
    {
        int key = 4567;
        Composite comp = new Composite();

        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        when(thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(mutator);

        wrapper.remove(key);

        verify(dao).removeColumnBatch(id, comp, mutator);
    }

    @Test
    public void should_remove_range() throws Exception
    {

        int start = 7, end = 5;

        Composite startComp = new Composite(), endComp = new Composite();

        when(
                thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                        BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING)).thenReturn(
                new Composite[]
                {
                        startComp,
                        endComp
                });
        when(thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(mutator);

        wrapper.remove(start, end, BoundingMode.INCLUSIVE_END_BOUND_ONLY);

        verify(thriftPropertyHelper).checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING,
                false);
        verify(dao).removeColumnRangeBatch(id, startComp, endComp, mutator);

    }

    @SuppressWarnings("rawtypes")
    @Test
    public void should_remove_first() throws Exception
    {
        when((Mutator) thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(
                mutator);
        wrapper.removeFirst(15);
        verify(dao).removeColumnRangeBatch(id, null, null, false, 15, mutator);
    }

    @Test
    public void should_remove_last() throws Exception
    {
        when(thriftImmediateFlushContext.getWideRowMutator("external_cf")).thenReturn(mutator);
        wrapper.removeLast(9);
        verify(dao).removeColumnRangeBatch(id, null, null, true, 9, mutator);
    }

    private JoinProperties prepareJoinProperties() throws Exception
    {
        EntityMeta joinEntityMeta = new EntityMeta();
        joinEntityMeta.setClassName("canonicalClassName");

        Method idGetter = UserBean.class.getDeclaredMethod("getUserId");
        PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
        idMeta.setType(PropertyType.SIMPLE);
        idMeta.setGetter(idGetter);
        joinEntityMeta.setIdMeta(idMeta);
        JoinProperties joinProperties = new JoinProperties();
        joinProperties.setEntityMeta(joinEntityMeta);

        return joinProperties;
    }
}
