package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftJoinEntityHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinEntityLoaderTest
{

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private ThriftEntityMapper mapper;

    @InjectMocks
    private ThriftJoinEntityLoader joinHelper;

    @Mock
    private ThriftGenericEntityDao dao;

    @Mock
    private EntityMeta joinMeta;

    @Mock
    private PropertyMeta joinIdMeta;

    @Captor
    ArgumentCaptor<UserBean> userCaptor;

    private List<Long> keys = Arrays.asList(11L);

    @Test
    public void should_load_join_entities() throws Exception
    {
        Method idSetter = UserBean.class.getDeclaredMethod("setUserId", Long.class);

        Composite start = new Composite();
        Composite end = new Composite();

        List<Pair<Composite, String>> columns1 = new ArrayList<Pair<Composite, String>>();
        columns1.add(Pair.create(start, "foo"));
        columns1.add(Pair.create(end, "bar"));

        List<Pair<Composite, String>> columns2 = new ArrayList<Pair<Composite, String>>();
        columns2.add(Pair.create(start, "john"));
        columns2.add(Pair.create(end, "helen"));

        Map<Long, List<Pair<Composite, String>>> rows = new HashMap<Long, List<Pair<Composite, String>>>();
        rows.put(11L, columns1);
        rows.put(12L, columns2);

        when(dao.eagerFetchEntities(keys)).thenReturn(rows);

        when(joinMeta.getIdMeta()).thenReturn(joinIdMeta);
        when(joinIdMeta.getSetter()).thenReturn(idSetter);

        Map<Long, UserBean> actual = joinHelper.loadJoinEntities(UserBean.class, keys, joinMeta,
                dao);

        verify(mapper).setEagerPropertiesToEntity(eq(11L), eq(columns1), eq(joinMeta),
                userCaptor.capture());
        verify(mapper).setEagerPropertiesToEntity(eq(12L), eq(columns2), eq(joinMeta),
                userCaptor.capture());

        verify(invoker).setValueToField(any(UserBean.class), eq(idSetter), eq(11L));
        verify(invoker).setValueToField(any(UserBean.class), eq(idSetter), eq(12L));

        assertThat(userCaptor.getAllValues()).hasSize(2);
        UserBean user1 = userCaptor.getAllValues().get(0);
        UserBean user2 = userCaptor.getAllValues().get(1);

        assertThat(actual.get(11L)).isSameAs(user1);
        assertThat(actual.get(12L)).isSameAs(user2);
    }

    @Test
    public void should_return_empty_map_when_no_join_entity_found() throws Exception
    {
        Map<Long, List<Pair<Composite, String>>> rows = new HashMap<Long, List<Pair<Composite, String>>>();
        List<Pair<Composite, String>> columns1 = new ArrayList<Pair<Composite, String>>();
        rows.put(11L, columns1);

        when(dao.eagerFetchEntities(keys)).thenReturn(rows);

        Map<Long, UserBean> actual = joinHelper.loadJoinEntities(UserBean.class, keys, joinMeta,
                dao);

        verifyZeroInteractions(mapper);

        verify(invoker, never()).setValueToField(any(UserBean.class), any(Method.class),
                any(Long.class));
        assertThat(actual).isEmpty();

    }
}
