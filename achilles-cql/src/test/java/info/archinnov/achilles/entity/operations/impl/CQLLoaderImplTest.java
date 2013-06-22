package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.MethodInvoker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;
import com.datastax.driver.core.Row;
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
    private MethodInvoker invoker;

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

    }

    @Test
    public void should_eager_load_entity() throws Exception
    {
        Long id = RandomUtils.nextLong();
        when(context.eagerLoadEntity()).thenReturn(row);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.getPrimaryKey()).thenReturn(id);

        CompleteBean actual = loaderImpl.eagerLoadEntity(context, CompleteBean.class);

        assertThat(actual).isInstanceOf(CompleteBean.class);

        verify(mapper).setEagerPropertiesToEntity(row, entityMeta, actual);
        verify(invoker).setValueToField(any(CompleteBean.class), eq(idMeta.getSetter()), eq(id));
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
        when(cqlRowInvoker.invokeOnRowForProperty(row, "name", Long.class)).thenReturn(11L);

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
        when(cqlRowInvoker.invokeOnRowForProperty(row, "name", Long.class)).thenReturn(null);

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
        when(cqlRowInvoker.invokeOnRowForList(row, "name", Long.class)).thenReturn(joinIds);

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
        when(cqlRowInvoker.invokeOnRowForList(row, "name", Long.class)).thenReturn(null);

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
        when(cqlRowInvoker.invokeOnRowForList(row, "name", Long.class)).thenReturn(
                new ArrayList<Long>());

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
        when(cqlRowInvoker.invokeOnRowForSet(row, "name", Long.class)).thenReturn(joinIds);

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
        when(cqlRowInvoker.invokeOnRowForSet(row, "name", Long.class)).thenReturn(null);

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
        when(cqlRowInvoker.invokeOnRowForSet(row, "name", Long.class)).thenReturn(
                new HashSet<Long>());

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_load_join_map_into_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        Map<Integer, Long> joinIds = ImmutableMap.of(11, 11L);
        when(cqlRowInvoker.invokeOnRowForMap(row, "name", Integer.class, Long.class)).thenReturn(
                joinIds);

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
                .noClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForMap(row, "name", Integer.class, Long.class)).thenReturn(
                null);

        loaderImpl.loadJoinPropertyIntoEntity(entityLoader, context, pm, entity);

        verify(mapper).setJoinValueToEntity(isNull(), eq(pm), eq(entity));
    }

    @Test
    public void should_not_load_join_map_into_entity_when_empty_id_map() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .noClass(Integer.class, UserBean.class)
                .field("name")
                .type(PropertyType.JOIN_MAP)
                .joinMeta(entityMeta)
                .build();

        when(context.loadProperty(pm)).thenReturn(row);
        when(cqlRowInvoker.invokeOnRowForMap(row, "name", Integer.class, Long.class)).thenReturn(
                new HashMap<Integer, Long>());

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
