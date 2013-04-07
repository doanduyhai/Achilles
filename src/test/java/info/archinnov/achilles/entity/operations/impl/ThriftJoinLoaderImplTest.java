package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.context.FlushContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import integration.tests.entity.User;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * JoinEntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinLoaderImplTest
{

	@InjectMocks
	private ThriftJoinLoaderImpl loader;

	@Mock
	private JoinEntityHelper joinHelper;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private CounterDao counterDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private FlushContext flushContext;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private PersistenceContext<Long> context;

	@SuppressWarnings("rawtypes")
	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.flushContext(flushContext) //
				.entityDao(entityDao) //
				.build();
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when((Mutator) flushContext.getEntityMutator("cf")).thenReturn(mutator);
	}

	@Test
	public void should_load_join_list() throws Exception
	{
		PropertyMeta<Void, User> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("name") //
				.accesors() //
				.build();

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
