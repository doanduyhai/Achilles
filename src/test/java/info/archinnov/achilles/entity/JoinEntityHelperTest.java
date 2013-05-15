package info.archinnov.achilles.entity;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * JoinEntityHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class JoinEntityHelperTest
{

	@Mock
	private AchillesEntityIntrospector introspector;

	@Mock
	private EntityMapper mapper;

	@InjectMocks
	private JoinEntityHelper joinHelper;

	@Mock
	private ThriftGenericEntityDao<Long> dao;

	@Mock
	private EntityMeta<Long> joinMeta;

	@Mock
	private PropertyMeta<Void, Long> joinIdMeta;

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
		columns1.add(new Pair<Composite, String>(start, "foo"));
		columns1.add(new Pair<Composite, String>(end, "bar"));

		List<Pair<Composite, String>> columns2 = new ArrayList<Pair<Composite, String>>();
		columns2.add(new Pair<Composite, String>(start, "john"));
		columns2.add(new Pair<Composite, String>(end, "helen"));

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

		verify(introspector).setValueToField(any(UserBean.class), eq(idSetter), eq(11L));
		verify(introspector).setValueToField(any(UserBean.class), eq(idSetter), eq(12L));

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
		verifyZeroInteractions(introspector);

		assertThat(actual).isEmpty();

	}
}
