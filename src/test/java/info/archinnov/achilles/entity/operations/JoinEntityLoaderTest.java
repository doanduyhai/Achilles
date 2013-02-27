package info.archinnov.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * JoinEntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class JoinEntityLoaderTest
{

	@InjectMocks
	private JoinEntityLoader loader;

	@Mock
	private JoinEntityHelper joinHelper;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private PropertyMeta<Void, UserBean> listMeta;

	@Mock
	private PropertyMeta<Void, UserBean> setMeta;

	@Mock
	private PropertyMeta<Integer, UserBean> mapMeta;

	@Mock
	private EntityMeta<Long> joinMeta;

	@Mock
	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Captor
	ArgumentCaptor<List<Long>> joinIdCaptor;

	@Mock
	private UserBean user1, user2;

	private Long key = 11L;

	@Test
	public void should_load_join_list() throws Exception
	{
		prepareTest(listMeta);

		List<UserBean> actual = loader.loadJoinListProperty(key, dao, listMeta);

		assertThat(actual).containsExactly(user1, user2);

	}

	@Test
	public void should_load_join_set() throws Exception
	{
		prepareTest(setMeta);

		Set<UserBean> actual = loader.loadJoinSetProperty(key, dao, setMeta);

		assertThat(actual).contains(user1, user2);

	}

	@Test
	public void should_load_join_map() throws Exception
	{
		when(mapMeta.getKeyClass()).thenReturn(Integer.class);
		KeyValue<Integer, UserBean> kv1 = new KeyValue<Integer, UserBean>(11, user1);
		KeyValue<Integer, UserBean> kv2 = new KeyValue<Integer, UserBean>(12, user2);

		when(mapMeta.getKeyValueFromString("11")).thenReturn(kv1);
		when(mapMeta.getKeyValueFromString("12")).thenReturn(kv2);

		prepareTest(mapMeta);

		when(joinIdMeta.getValueFromString(user1)).thenReturn(11L);
		when(joinIdMeta.getValueFromString(user2)).thenReturn(12L);

		Map<Integer, UserBean> actual = loader.loadJoinMapProperty(key, dao, mapMeta);

		assertThat(actual.get(11)).isSameAs(user1);
		assertThat(actual.get(12)).isSameAs(user2);

	}

	@SuppressWarnings("unchecked")
	private void prepareTest(PropertyMeta<?, UserBean> propertyMeta)
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(start, "11"));
		columns.add(new Pair<DynamicComposite, String>(end, "12"));

		when(dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE)).thenReturn(columns);

		when((EntityMeta<Long>) propertyMeta.joinMeta()).thenReturn(joinMeta);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);

		when(joinIdMeta.getValueFromString("11")).thenReturn(11L);
		when(joinIdMeta.getValueFromString("12")).thenReturn(12L);

		Map<Long, UserBean> map = new HashMap<Long, UserBean>();
		map.put(11L, user1);
		map.put(12L, user2);

		when(joinHelper.loadJoinEntities(eq(UserBean.class), joinIdCaptor.capture(), eq(joinMeta)))
				.thenReturn(map);
	}
}
