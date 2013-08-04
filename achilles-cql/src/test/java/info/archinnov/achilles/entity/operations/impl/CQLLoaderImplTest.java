package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * CQLLoaderImplTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLLoaderImplTest
{
    @InjectMocks
    private CQLLoaderImpl loaderImpl;

    @Mock
    private CQLEntityMapper mapper;

    @Mock
    private CQLEntityLoader entityLoader;

    @Mock
    private CQLRowMethodInvoker cqlRowInvoker;

    @Mock
    private Row row;

    @Mock
    private CQLPersistenceContext context;

    @Mock
    private EntityMeta entityMeta;

    @Captor
    private ArgumentCaptor<List<UserBean>> listCaptor;

    @Captor
    private ArgumentCaptor<Set<UserBean>> setCaptor;

    @Captor
    private ArgumentCaptor<Map<Integer, UserBean>> mapCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private PropertyMeta<?, Long> idMeta;

    @Before
    public void setUp() throws Exception
    {
        when(context.getEntityMeta()).thenReturn(entityMeta);
        idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.ID)
                .accessors()
                .build();

        when((PropertyMeta) entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.isClusteredCounter()).thenReturn(false);
    }

    @Test
    public void should_eager_load_entity() throws Exception
    {
        Long id = RandomUtils.nextLong();
        when(context.eagerLoadEntity()).thenReturn(row);
        when(context.getEntityMeta()).thenReturn(entityMeta);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isInstanceOf(CompleteBean.class);

        verify(mapper).setEagerPropertiesToEntity(row, entityMeta, actual);
    }

    @Test
    public void should_return_null_for_eager_load_when_not_found() throws Exception
    {
        when(context.eagerLoadEntity()).thenReturn(null);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isNull();
        verifyZeroInteractions(mapper);
    }

    @Test
    public void should_eager_load_clustered_counter_entity_with_runtime_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .build();
        when(entityMeta.isClusteredCounter()).thenReturn(true);
        when(entityMeta.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(counterValue);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isInstanceOf(CompleteBean.class);

        verifyZeroInteractions(mapper);
    }

    @Test
    public void should_eager_load_clustered_counter_entity_with_default_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM))
                .build();
        when(entityMeta.isClusteredCounter()).thenReturn(true);
        when(entityMeta.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(counterValue);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isInstanceOf(CompleteBean.class);

        verifyZeroInteractions(mapper);
    }

    @Test
    public void should_return_null_for_eager_load_clusterd_counter_when_not_found() throws Exception
    {
        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .build();

        when(entityMeta.isClusteredCounter()).thenReturn(true);
        when(entityMeta.getFirstMeta()).thenReturn((PropertyMeta) counterMeta);
        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
        when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(null);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isNull();
    }

    @Test
    public void should_load_property_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();

        CompleteBean entity = new CompleteBean();
        when(context.loadProperty(pm)).thenReturn(row);

        loaderImpl.loadPropertyIntoEntity(context, pm, entity);

        verify(mapper).setPropertyToEntity(row, pm, entity);
    }

    @Test
    public void should_load_join_simple_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "name", Long.class)).thenReturn(11L);

        UserBean userBean = new UserBean();
        when(entityLoader.load(any(CQLPersistenceContext.class), eq(UserBean.class))).thenReturn(
                userBean);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(userBean, pm, entity);
    }

    @Test
    public void should_not_load_join_simple_into_entity_when_null() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "name", Long.class)).thenReturn(null);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_load_join_list_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_LIST)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        List<Long> joinIds = Arrays.asList(11L);
        when(cqlRowInvoker.invokeOnRowForList(row, pm, "name", Long.class)).thenReturn((List) joinIds);

        UserBean userBean = new UserBean();
        when(entityLoader.load(any(CQLPersistenceContext.class), eq(UserBean.class))).thenReturn(
                userBean);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(listCaptor.capture(), eq(pm), eq(entity));
        assertThat(listCaptor.getValue()).containsExactly(userBean);
    }

    @Test
    public void should_not_load_join_list_into_entity_when_null() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_LIST)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForList(row, pm, "name", Long.class)).thenReturn(null);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_not_load_join_list_into_entity_empty_id_list() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_LIST)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForList(row, pm, "name", Long.class)).thenReturn(
                new ArrayList());

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_load_join_set_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_SET)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        Set<Long> joinIds = Sets.newHashSet(11L);
        when(cqlRowInvoker.invokeOnRowForSet(row, pm, "name", Long.class)).thenReturn((Set) joinIds);

        UserBean userBean = new UserBean();
        when(entityLoader.load(any(CQLPersistenceContext.class), eq(UserBean.class))).thenReturn(
                userBean);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(setCaptor.capture(), eq(pm), eq(entity));
        assertThat(setCaptor.getValue()).containsExactly(userBean);
    }

    @Test
    public void should_not_load_join_set_into_entity_when_null() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_SET)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForSet(row, pm, "name", Long.class)).thenReturn(null);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_not_load_join_set_into_entity_when_empty_id_set() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_SET)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForSet(row, pm, "name", Long.class)).thenReturn(
                new HashSet());

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_load_join_map_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        Map<Integer, Long> joinIds = ImmutableMap.of(11, 11L);
        when(cqlRowInvoker.invokeOnRowForMap(row, pm, "name", Integer.class, Long.class)).thenReturn(
                (Map) joinIds);

        UserBean userBean = new UserBean();
        when(entityLoader.load(any(CQLPersistenceContext.class), eq(UserBean.class))).thenReturn(
                userBean);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(mapCaptor.capture(), eq(pm), eq(entity));
        assertThat(mapCaptor.getValue()).containsKey(11);
        assertThat(mapCaptor.getValue()).containsValue(userBean);
    }

    @Test
    public void should_not_load_join_map_into_entity_when_null() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForMap(row, pm, "name", Integer.class, Long.class)).thenReturn(
                null);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_not_load_join_map_into_entity_when_empty_id_map() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForMap(row, pm, "name", Integer.class, Long.class)).thenReturn(
                new HashMap());

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_load_null_when_not_join_type() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(UserBean.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .joinMeta(entityMeta)
                .build();
        when(context.loadProperty(pm)).thenReturn(row);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));

        verifyZeroInteractions(cqlRowInvoker);
    }
}
