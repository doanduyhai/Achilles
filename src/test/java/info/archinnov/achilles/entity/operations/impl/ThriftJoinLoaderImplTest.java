package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
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
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.ImmutableMap;

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
	private ThriftJoinLoaderImpl thriftJoinLoader;

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
	private Map<String, GenericEntityDao<?>> entityDaosMap;

	@Mock
	private FlushContext flushContext;

	@Mock
	private GenericEntityDao<Long> joinEntityDao;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private PersistenceContext<Long> context;

	@Captor
	private ArgumentCaptor<List<Long>> listCaptor;

	private ObjectMapper objectMapper = new ObjectMapper();

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.flushContext(flushContext) //
				.entityDao(entityDao) //
				.entityDaosMap(entityDaosMap) //
				.build();
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when((Mutator) flushContext.getEntityMutator("cf")).thenReturn(mutator);
		when((GenericEntityDao<Long>) entityDaosMap.get("join_cf")).thenReturn(joinEntityDao);

	}

	@Test
	public void should_load_join_list() throws Exception
	{
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.accesors() //
				.type(PropertyType.SIMPLE) //
				.build();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setColumnFamilyName("join_cf");
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		PropertyMeta<Void, UserBean> propertyMeta = new PropertyMeta<Void, UserBean>();
		propertyMeta.setJoinProperties(joinProperties);
		propertyMeta.setValueClass(UserBean.class);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(start, "11"));
		columns.add(new Pair<DynamicComposite, String>(end, "12"));
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		UserBean user1 = new UserBean();
		UserBean user2 = new UserBean();
		Map<Long, UserBean> joinEntitiesMap = ImmutableMap.of(11L, user1, 12L, user2);

		when(
				joinHelper.loadJoinEntities(eq(UserBean.class), listCaptor.capture(), eq(joinMeta),
						eq(joinEntityDao))).thenReturn(joinEntitiesMap);

		List<UserBean> actual = thriftJoinLoader.loadJoinListProperty(context, propertyMeta);

		assertThat(actual).containsExactly(user1, user2);
		assertThat(listCaptor.getValue()).containsExactly(11L, 12L);
	}

	@Test
	public void should_load_join_set() throws Exception
	{
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.accesors() //
				.type(PropertyType.SIMPLE) //
				.build();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setColumnFamilyName("join_cf");
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		PropertyMeta<Void, UserBean> propertyMeta = new PropertyMeta<Void, UserBean>();
		propertyMeta.setJoinProperties(joinProperties);
		propertyMeta.setValueClass(UserBean.class);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(start, "11"));
		columns.add(new Pair<DynamicComposite, String>(end, "12"));
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		UserBean user1 = new UserBean();
		UserBean user2 = new UserBean();
		Map<Long, UserBean> joinEntitiesMap = ImmutableMap.of(11L, user1, 12L, user2);

		when(
				joinHelper.loadJoinEntities(eq(UserBean.class), listCaptor.capture(), eq(joinMeta),
						eq(joinEntityDao))).thenReturn(joinEntitiesMap);

		Set<UserBean> actual = thriftJoinLoader.loadJoinSetProperty(context, propertyMeta);

		assertThat(actual).contains(user1, user2);
		assertThat(listCaptor.getValue()).containsExactly(11L, 12L);
	}

	@Test
	public void should_load_join_map() throws Exception
	{
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.accesors() //
				.type(PropertyType.SIMPLE) //
				.build();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setColumnFamilyName("join_cf");
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		PropertyMeta<Integer, UserBean> propertyMeta = new PropertyMeta<Integer, UserBean>();
		propertyMeta.setJoinProperties(joinProperties);
		propertyMeta.setKeyClass(Integer.class);
		propertyMeta.setValueClass(UserBean.class);
		propertyMeta.setObjectMapper(objectMapper);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(start,
				writeString(new KeyValue<Integer, String>(11, "11"))));
		columns.add(new Pair<DynamicComposite, String>(end,
				writeString(new KeyValue<Integer, String>(12, "12"))));
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		UserBean user1 = new UserBean();
		UserBean user2 = new UserBean();
		Map<Long, UserBean> joinEntitiesMap = ImmutableMap.of(11L, user1, 12L, user2);
		when(
				joinHelper.loadJoinEntities(eq(UserBean.class), listCaptor.capture(), eq(joinMeta),
						eq(joinEntityDao))).thenReturn(joinEntitiesMap);

		Map<Integer, UserBean> actual = thriftJoinLoader.loadJoinMapProperty(context, propertyMeta);

		assertThat(actual.get(11)).isSameAs(user1);
		assertThat(actual.get(12)).isSameAs(user2);
		assertThat(listCaptor.getValue()).containsExactly(11L, 12L);
	}

	private String writeString(Object value) throws Exception
	{
		return objectMapper.writerWithType(KeyValue.class).writeValueAsString(value);
	}
}
