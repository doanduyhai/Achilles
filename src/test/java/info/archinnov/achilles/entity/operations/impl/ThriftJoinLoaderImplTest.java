package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftJoinEntityHelper;
import info.archinnov.achilles.entity.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
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
	private ThriftJoinEntityHelper joinHelper;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private ThriftGenericEntityDao<Long> entityDao;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private Map<String, ThriftGenericEntityDao<?>> entityDaosMap;

	@Mock
	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	@Mock
	private ThriftGenericEntityDao<Long> joinEntityDao;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private ThriftPersistenceContext<Long> context;

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
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.thriftImmediateFlushContext(thriftImmediateFlushContext) //
				.entityDao(entityDao) //
				.entityDaosMap(entityDaosMap) //
				.build();
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when((Mutator) thriftImmediateFlushContext.getEntityMutator("cf")).thenReturn(mutator);
		when((ThriftGenericEntityDao<Long>) entityDaosMap.get("join_cf")).thenReturn(joinEntityDao);

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

		Composite start = new Composite();
		Composite end = new Composite();

		when(compositeFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();
		columns.add(new Pair<Composite, String>(start, "11"));
		columns.add(new Pair<Composite, String>(end, "12"));
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

		Composite start = new Composite();
		Composite end = new Composite();

		when(compositeFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();
		columns.add(new Pair<Composite, String>(start, "11"));
		columns.add(new Pair<Composite, String>(end, "12"));
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

		Composite start = new Composite();
		Composite end = new Composite();

		when(compositeFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();
		columns.add(new Pair<Composite, String>(start, writeString(new KeyValue<Integer, String>(
				11, "11"))));
		columns.add(new Pair<Composite, String>(end, writeString(new KeyValue<Integer, String>(12,
				"12"))));
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
