package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.CascadeType;
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
 * CQLMergerImplTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLMergerImplTest
{
    @InjectMocks
    private CQLMergerImpl mergerImpl = new CQLMergerImpl();

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private CQLEntityMerger entityMerger;

    @Mock
    private CQLPersistenceContext context;

    @Mock
    private CQLPersistenceContext joinContext;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private EntityMeta joinMeta;

    @Captor
    private ArgumentCaptor<List<PropertyMeta>> pmCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private List<PropertyMeta> joinPMs = new ArrayList<PropertyMeta>();

    private PropertyMeta idMeta;

    @Before
    public void setUp() throws Exception
    {
        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(entityMeta);

        idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .build();

        joinPMs.clear();
    }

    @Test
    public void should_merge() throws Exception
    {
        PropertyMeta ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .accessors()
                .build();
        Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();
        dirtyMap.put(idMeta.getGetter(), idMeta);
        dirtyMap.put(ageMeta.getGetter(), ageMeta);

        mergerImpl.merge(context, dirtyMap);

        assertThat(dirtyMap).isEmpty();

        verify(context).pushUpdateStatement(pmCaptor.capture());

        assertThat(pmCaptor.getValue()).containsExactly(ageMeta, idMeta);
    }

    @Test
    public void should_not_merge_when_empty_dirty_map() throws Exception
    {
        Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();
        mergerImpl.merge(context, dirtyMap);

        verifyZeroInteractions(context);
    }

    @Test
    public void should_cascade_to_join_simple() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .cascadeType(CascadeType.ALL)
                .build();

        joinPMs.add(joinSimpleMeta);

        Object user = new UserBean();
        when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(user);

        when(context.createContextForJoin(joinMeta, user)).thenReturn(joinContext);

        mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

        verify(entityMerger).merge(joinContext, user);
    }

    @Test
    public void should_cascade_to_join_collection() throws Exception
    {
        PropertyMeta joinListMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_LIST)
                .cascadeType(CascadeType.ALL)
                .joinMeta(joinMeta)
                .build();

        joinPMs.add(joinListMeta);

        UserBean user1 = new UserBean();
        UserBean user2 = new UserBean();
        List<UserBean> users = Arrays.asList(user1, user2, null);
        when(invoker.getValueFromField(entity, joinListMeta.getGetter())).thenReturn(users);

        when(context.createContextForJoin(joinMeta, user1)).thenReturn(joinContext);
        when(context.createContextForJoin(joinMeta, user2)).thenReturn(joinContext);

        mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

        verify(entityMerger).merge(joinContext, user1);
        verify(entityMerger).merge(joinContext, user2);
    }

    @Test
    public void should_cascade_to_join_map() throws Exception
    {
        PropertyMeta joinMapMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_MAP)
                .cascadeType(CascadeType.ALL)
                .joinMeta(joinMeta)
                .build();

        joinPMs.add(joinMapMeta);

        UserBean user1 = new UserBean();
        UserBean user2 = new UserBean();
        Map<Integer, UserBean> users = ImmutableMap.of(1, user1, 2, user2);
        when(invoker.getValueFromField(entity, joinMapMeta.getGetter())).thenReturn(users);

        when(context.createContextForJoin(joinMeta, user1)).thenReturn(joinContext);
        when(context.createContextForJoin(joinMeta, user2)).thenReturn(joinContext);

        mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

        verify(entityMerger).merge(joinContext, user1);
        verify(entityMerger).merge(joinContext, user2);
    }

    @Test
    public void should_not_cascade_if_null() throws Exception
    {
        PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .build();

        joinPMs.add(joinSimpleMeta);

        mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

        verifyZeroInteractions(entityMerger);
    }

}
