package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.KeyValue;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftLoaderImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftLoaderImplTest
{

    @InjectMocks
    private ThriftLoaderImpl loaderImpl;

    @Mock
    private ThriftEntityLoader loader;

    @Mock
    private ThriftEntityMapper mapper;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private ThriftCompositeFactory compositeFactory;

    @Mock
    private ThriftCompositeTransformer compositeTransformer;

    private EntityMeta entityMeta;

    @Mock
    private ThriftGenericEntityDao entityDao;

    @Mock
    private ThriftGenericWideRowDao wideRowDao;

    @Mock
    private ThriftImmediateFlushContext flushContext;

    @Mock
    private ThriftCounterDao thriftCounterDao;

    @Mock
    private ThriftConsistencyLevelPolicy policy;

    @Captor
    ArgumentCaptor<CompleteBean> beanCaptor;

    @Captor
    ArgumentCaptor<ThriftPersistenceContext> contextCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private ObjectMapper objectMapper = new ObjectMapper();

    private PropertyMeta<Void, Long> idMeta;

    private ThriftPersistenceContext context;

    @Before
    public void setUp() throws Throwable
    {
        idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .build();

        entityMeta = new EntityMeta();
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Long.class);
        context = buildPersistenceContext(entityMeta);
    }

    private ThriftPersistenceContext buildPersistenceContext(EntityMeta entityMeta)
    {
        return ThriftPersistenceContextTestBuilder
                .context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
                .entity(entity)
                .thriftImmediateFlushContext(flushContext)
                .entityDao(entityDao)
                .wideRowDao(wideRowDao)
                .build();
    }

    @Test
    public void should_load() throws Exception
    {

        Composite comp = new Composite();
        List<Pair<Composite, String>> values = new ArrayList<Pair<Composite, String>>();
        values.add(Pair.create(comp, "value"));

        when(entityDao.eagerFetchEntity(entity.getId())).thenReturn(values);

        CompleteBean actual = loaderImpl.load(context, CompleteBean.class);

        verify(mapper).setEagerPropertiesToEntity(eq(entity.getId()), eq(values), eq(entityMeta),
                beanCaptor.capture());
        verify(invoker).setValueToField(beanCaptor.capture(), eq(idMeta.getSetter()),
                eq(entity.getId()));

        assertThat(beanCaptor.getAllValues()).containsExactly(actual, actual);
    }

    @Test
    public void should_load_clustered_entity() throws Exception
    {
        Composite comp = new Composite();
        Object clusteredValue = "clusteredValue";
        BeanWithClusteredId expected = new BeanWithClusteredId();
        HColumn<Composite, Object> hCol = new HColumnImpl<Composite, Object>(comp, clusteredValue,
                10);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");
        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(hCol);
        when(
                compositeTransformer.buildClusteredEntity(BeanWithClusteredId.class, context, hCol))
                .thenReturn(expected);

        BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_return_null_when_no_clustered_entity() throws Exception
    {
        Composite comp = new Composite();

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();
        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(null);

        assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

        verifyZeroInteractions(compositeTransformer);
    }

    @Test
    public void should_load_join_clustered_entity() throws Exception
    {
        Composite comp = new Composite();
        Object clusteredValue = "clusteredValue";
        BeanWithClusteredId expected = new BeanWithClusteredId();
        HColumn<Composite, Object> hCol = new HColumnImpl<Composite, Object>(comp, clusteredValue,
                10);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .type(JOIN_SIMPLE)
                .build();

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");
        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(hCol);
        when(mapper.initClusteredEntity(BeanWithClusteredId.class, idMeta, primaryKey)).thenReturn(
                expected);

        BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_return_null_when_no_join_clustered_entity() throws Exception
    {
        Composite comp = new Composite();

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .type(JOIN_SIMPLE)
                .build();

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(null);

        assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

        verifyZeroInteractions(compositeTransformer);
    }

    @Test
    public void should_load_counter_clustered_entity() throws Exception
    {
        Composite comp = new Composite();
        BeanWithClusteredId expected = new BeanWithClusteredId();
        HCounterColumn<Composite> hCounterCol = new HCounterColumnImpl<Composite>(comp, 10L);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        PropertyMeta<Void, Long> pm = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(COUNTER)
                .build();

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");
        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getCounterColumn(partitionKey, comp)).thenReturn(hCounterCol);
        when(
                compositeTransformer.buildClusteredEntityWithIdOnly(BeanWithClusteredId.class,
                        context, hCounterCol.getName().getComponents()))
                .thenReturn(expected);

        BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_return_null_when_no_counter_clustered_entity() throws Exception
    {
        Composite comp = new Composite();

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(COUNTER)
                .build();

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta, "pm", pm));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList(pm));
        entityMeta.setFirstMeta(pm);

        context = buildPersistenceContext(entityMeta);

        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getCounterColumn(partitionKey, comp)).thenReturn(null);

        assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

        verifyZeroInteractions(compositeTransformer);
    }

    @Test
    public void should_load_value_less_clustered_entity() throws Exception
    {
        Composite comp = new Composite();
        Object clusteredValue = "";
        BeanWithClusteredId expected = new BeanWithClusteredId();
        HColumn<Composite, Object> hCol = new HColumnImpl<Composite, Object>(comp, clusteredValue,
                10);

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta));
        entityMeta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta<?, ?>> asList());

        context = buildPersistenceContext(entityMeta);

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");
        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(hCol);
        when(
                compositeTransformer.buildClusteredEntityWithIdOnly(BeanWithClusteredId.class, context, hCol
                        .getName().getComponents()))
                .thenReturn(expected);

        BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_return_null_when_no_value_less_clustered_entity() throws Exception
    {
        Composite comp = new Composite();

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder //
                .valueClass(CompoundKey.class)
                .field("id")
                .type(EMBEDDED_ID)
                .build();

        Long partitionKey = RandomUtils.nextLong();
        CompoundKey primaryKey = new CompoundKey(partitionKey, "name");

        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(CompoundKey.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta));

        context = buildPersistenceContext(entityMeta);

        context.setPrimaryKey(primaryKey);

        when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
        when(invoker.getPartitionKey(primaryKey, idMeta)).thenReturn(partitionKey);
        when(wideRowDao.getColumn(partitionKey, comp)).thenReturn(null);

        assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

        verifyZeroInteractions(compositeTransformer);
    }

    @Test
    public void should_load_simple_property_for_entity() throws Exception
    {

        PropertyMeta<Void, String> nameMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .build();

        Composite comp = new Composite();
        when(compositeFactory.createBaseForGet(nameMeta)).thenReturn(comp);
        when(entityDao.getValue(entity.getId(), comp)).thenReturn("name_xyz");

        String actual = loaderImpl.loadSimpleProperty(context, nameMeta);
        assertThat(actual).isEqualTo("name_xyz");
    }

    @Test
    public void should_load_simple_property_for_clustered_entity() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        CompoundKey embeddedId = new CompoundKey(partitionKey, "name");
        Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        PropertyMeta<Void, CompoundKey> embeddedIdMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .compGetters(userIdGetter)
                .build();

        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .build();

        entityMeta.setIdMeta(embeddedIdMeta);
        entityMeta.setClusteredEntity(true);
        context = buildPersistenceContext(entityMeta);
        context.setPrimaryKey(embeddedId);

        Composite comp = new Composite();
        when(compositeFactory.createBaseForClusteredGet(embeddedId, embeddedIdMeta)).thenReturn(
                comp);

        when(wideRowDao.getValue(partitionKey, comp)).thenReturn("name_xyz");

        String actual = loaderImpl.loadSimpleProperty(context, pm);
        assertThat(actual).isEqualTo("name_xyz");
    }

    @Test
    public void should_load_list() throws Exception
    {
        PropertyMeta<Void, String> listMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("friends")
                .accessors()
                .build();

        Composite start = new Composite(), end = new Composite();
        List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
        columns.add(Pair.<Composite, Object> create(start, "foo"));
        columns.add(Pair.<Composite, Object> create(end, "bar"));

        when(compositeFactory.createBaseForQuery(listMeta, EQUAL)).thenReturn(start);
        when(compositeFactory.createBaseForQuery(listMeta, GREATER_THAN_EQUAL)).thenReturn(end);
        when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
                .thenReturn(columns);

        List<String> actual = loaderImpl.loadListProperty(context, listMeta);
        assertThat(actual).containsExactly("foo", "bar");
    }

    @Test
    public void should_load_set() throws Exception
    {
        PropertyMeta<Void, String> setMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("followers")
                .accessors()
                .build();

        Composite start = new Composite(), end = new Composite();
        List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
        columns.add(Pair.<Composite, Object> create(start, "John"));
        columns.add(Pair.<Composite, Object> create(end, "Helen"));

        when(compositeFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
        when(compositeFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL)).thenReturn(end);
        when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
                .thenReturn(columns);

        Set<String> actual = loaderImpl.loadSetProperty(context, setMeta);
        assertThat(actual).containsExactly("John", "Helen");
    }

    @Test
    public void should_load_map() throws Exception
    {
        PropertyMeta<Integer, UserBean> setMeta = PropertyMetaTestBuilder
                //
                .completeBean(Integer.class, UserBean.class)
                .field("usersMap")
                .type(PropertyType.MAP)
                .accessors()
                .build();

        Composite start = new Composite(), end = new Composite();
        List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

        UserBean user1 = new UserBean(), user2 = new UserBean();
        user1.setName("user1");
        user2.setName("user2");

        columns.add(Pair.<Composite, Object> create(start, writeToString(new KeyValue<Integer, UserBean>(1, user1))));
        columns.add(Pair.<Composite, Object> create(end, writeToString(new KeyValue<Integer, UserBean>(2, user2))));

        when(compositeFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
        when(compositeFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL)).thenReturn(end);
        when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
                .thenReturn(columns);

        Map<Integer, UserBean> actual = loaderImpl.loadMapProperty(context, setMeta);
        assertThat(actual).hasSize(2);
        assertThat(actual.get(1).getName()).isEqualTo("user1");
        assertThat(actual.get(2).getName()).isEqualTo("user2");
    }

    @Test
    public void should_load_join_simple_for_entity() throws Exception
    {
        String stringJoinId = RandomUtils.nextLong() + "";
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(idMeta);

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                //
                .completeBean(Integer.class, UserBean.class)
                .field("user")
                .joinMeta(joinMeta)
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .build();

        UserBean user = new UserBean();
        Composite comp = new Composite();
        when(compositeFactory.createBaseForGet(propertyMeta)).thenReturn(comp);
        when(entityDao.getValue(entity.getId(), comp)).thenReturn(stringJoinId);
        when(loader.load(contextCaptor.capture(), eq(UserBean.class))).thenReturn(user);

        UserBean actual = loaderImpl.loadJoinSimple(context, propertyMeta, loader);
        assertThat(actual).isSameAs(user);
        assertThat(contextCaptor.getValue().getPrimaryKey()).isEqualTo(new Long(stringJoinId));
        assertThat(contextCaptor.getValue().getEntityMeta()).isSameAs(joinMeta);
    }

    @Test
    public void should_load_join_simple_for_clustered_entity() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        CompoundKey embeddedId = new CompoundKey(partitionKey, "name");
        Long joinId = RandomUtils.nextLong();

        PropertyMeta<Void, CompoundKey> embeddedIdMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .build();

        entityMeta.setIdMeta(embeddedIdMeta);
        entityMeta.setClusteredEntity(true);
        context.setPrimaryKey(embeddedId);

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(idMeta);

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .joinMeta(joinMeta)
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .build();

        UserBean user = new UserBean();
        Composite comp = new Composite();
        when(invoker.getPartitionKey(embeddedId, embeddedIdMeta)).thenReturn(partitionKey);
        when(compositeFactory.createBaseForClusteredGet(embeddedId, embeddedIdMeta)).thenReturn(
                comp);
        when(wideRowDao.getValue(partitionKey, comp)).thenReturn(joinId);
        when(loader.load(contextCaptor.capture(), eq(UserBean.class))).thenReturn(user);

        UserBean actual = loaderImpl.loadJoinSimple(context, pm, loader);
        assertThat(actual).isSameAs(user);
        assertThat(contextCaptor.getValue().getPrimaryKey()).isEqualTo(joinId);
        assertThat(contextCaptor.getValue().getEntityMeta()).isSameAs(joinMeta);
    }

    @Test
    public void should_return_null_when_no_join_entity() throws Exception
    {
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(idMeta);

        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder
                //
                .completeBean(Integer.class, UserBean.class)
                .field("user")
                .joinMeta(joinMeta)
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .build();

        Composite comp = new Composite();
        when(compositeFactory.createBaseForGet(propertyMeta)).thenReturn(comp);
        when(entityDao.getValue(entity.getId(), comp)).thenReturn(null);

        UserBean actual = loaderImpl.loadJoinSimple(context, propertyMeta, loader);
        assertThat(actual).isNull();

    }

    private String writeToString(KeyValue<Integer, UserBean> keyValue) throws Exception
    {
        return objectMapper.writeValueAsString(keyValue);
    }
}
