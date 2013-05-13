package info.archinnov.achilles.entity.operations;

import static javax.persistence.CascadeType.MERGE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityMergerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityMergerTest
{

	@InjectMocks
	private EntityMerger merger;

	@Mock
	private EntityPersister persister;

	@Mock
	private JpaEntityInterceptor<Object, CompleteBean> interceptor;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private ThriftGenericEntityDao<Long> dao;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Captor
	ArgumentCaptor<ThriftPersistenceContext<Long>> contextCaptor;

	@Captor
	ArgumentCaptor<List<UserBean>> listUserBeanCaptor;

	@Captor
	ArgumentCaptor<Set<UserBean>> setUserBeanCaptor;

	@Captor
	ArgumentCaptor<Map<Integer, UserBean>> mapUserBeanCaptor;

	private ThriftPersistenceContext<Long> context;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

	private Map<Method, PropertyMeta<?, ?>> dirtyMap = new HashMap<Method, PropertyMeta<?, ?>>();

	@SuppressWarnings("rawtypes")
	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity).build();

		when(proxifier.isProxy(entity)).thenReturn(true);
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when((JpaEntityInterceptor) proxifier.getInterceptor(entity)).thenReturn(interceptor);
		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		dirtyMap.clear();
	}

	@Test
	public void should_merge_proxy_with_multi_value_dirty() throws Exception
	{

		when(entityMeta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta<?, ?>>());

		PropertyMeta<Void, String> friendsMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("friends") //
				.accesors() //
				.type(PropertyType.LIST) //
				.build();

		dirtyMap.put(friendsMeta.getGetter(), friendsMeta);

		CompleteBean mergedEntity = merger.mergeEntity(context);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).removePropertyBatch(context, friendsMeta);
		verify(persister).persistProperty(context, friendsMeta);
		assertThat(dirtyMap).isEmpty();
	}

	@Test
	public void should_merge_proxy_with_no_dirty() throws Exception
	{
		when(entityMeta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta<?, ?>>());

		CompleteBean mergedEntity = merger.mergeEntity(context);

		assertThat(mergedEntity).isSameAs(entity);
		verifyZeroInteractions(persister);
		assertThat(dirtyMap).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_merge_proxy_with_join_entity() throws Exception
	{
		PropertyMeta<Void, UserBean> joinPropertyMeta = prepareJoinPropertyMeta();
		joinPropertyMeta.setType(PropertyType.JOIN_SIMPLE);

		UserBean userBean = new UserBean();
		userBean.setUserId(10L);

		when(introspector.getValueFromField(entity, joinPropertyMeta.getGetter())).thenReturn(
				userBean);
		when(proxifier.isProxy(userBean)).thenReturn(false);
		when(proxifier.buildProxy(eq(userBean), any(ThriftPersistenceContext.class)))
				.thenReturn(userBean);

		CompleteBean actual = merger.mergeEntity(context);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(contextCaptor.capture());
		verify(introspector).setValueToField(entity, joinPropertyMeta.getSetter(), userBean);
		verify(interceptor).setTarget(entity);
		ThriftPersistenceContext<Long> joinContext = contextCaptor.getValue();
		assertThat(joinContext.getEntity()).isSameAs(userBean);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_merge_proxy_with_list_of_join_entity() throws Exception
	{
		PropertyMeta<Void, UserBean> joinPropertyMeta = prepareJoinPropertyMeta();
		joinPropertyMeta.setType(PropertyType.JOIN_LIST);

		UserBean userBean = new UserBean();
		userBean.setUserId(10L);

		List<UserBean> userBeans = Arrays.asList(userBean);
		when(introspector.getValueFromField(entity, joinPropertyMeta.getGetter())).thenReturn(
				userBeans);
		when(proxifier.isProxy(userBean)).thenReturn(false);
		when(proxifier.buildProxy(eq(userBean), any(ThriftPersistenceContext.class)))
				.thenReturn(userBean);

		CompleteBean actual = merger.mergeEntity(context);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(contextCaptor.capture());
		verify(introspector).setValueToField(eq(entity), eq(joinPropertyMeta.getSetter()),
				listUserBeanCaptor.capture());
		verify(interceptor).setTarget(entity);
		ThriftPersistenceContext<Long> joinContext = contextCaptor.getValue();
		assertThat(joinContext.getEntity()).isSameAs(userBean);
		assertThat(listUserBeanCaptor.getValue()).containsExactly(userBean);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_merge_proxy_with_set_of_join_entity() throws Exception
	{
		PropertyMeta<Void, UserBean> joinPropertyMeta = prepareJoinPropertyMeta();
		joinPropertyMeta.setType(PropertyType.JOIN_SET);

		UserBean userBean = new UserBean();
		userBean.setUserId(10L);

		Set<UserBean> userBeans = new HashSet<UserBean>();
		userBeans.add(userBean);

		when(introspector.getValueFromField(entity, joinPropertyMeta.getGetter())).thenReturn(
				userBeans);
		when(proxifier.isProxy(userBean)).thenReturn(false);
		when(proxifier.buildProxy(eq(userBean), any(ThriftPersistenceContext.class)))
				.thenReturn(userBean);

		CompleteBean actual = merger.mergeEntity(context);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(contextCaptor.capture());
		verify(introspector).setValueToField(eq(entity), eq(joinPropertyMeta.getSetter()),
				setUserBeanCaptor.capture());
		verify(interceptor).setTarget(entity);
		ThriftPersistenceContext<Long> joinContext = contextCaptor.getValue();
		assertThat(joinContext.getEntity()).isSameAs(userBean);
		assertThat(setUserBeanCaptor.getValue()).containsExactly(userBean);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_merge_proxy_with_map_of_join_entity() throws Exception
	{
		PropertyMeta<Void, UserBean> joinPropertyMeta = prepareJoinPropertyMeta();
		joinPropertyMeta.setType(PropertyType.JOIN_SET);

		UserBean userBean = new UserBean();
		userBean.setUserId(10L);

		Set<UserBean> userBeans = new HashSet<UserBean>();
		userBeans.add(userBean);

		when(introspector.getValueFromField(entity, joinPropertyMeta.getGetter())).thenReturn(
				userBeans);
		when(proxifier.isProxy(userBean)).thenReturn(false);
		when(proxifier.buildProxy(eq(userBean), any(ThriftPersistenceContext.class)))
				.thenReturn(userBean);

		CompleteBean actual = merger.mergeEntity(context);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(contextCaptor.capture());
		verify(introspector).setValueToField(eq(entity), eq(joinPropertyMeta.getSetter()),
				setUserBeanCaptor.capture());
		verify(interceptor).setTarget(entity);
		ThriftPersistenceContext<Long> joinContext = contextCaptor.getValue();
		assertThat(joinContext.getEntity()).isSameAs(userBean);
		assertThat(setUserBeanCaptor.getValue()).containsExactly(userBean);
	}

	// /////////////////////
	private PropertyMeta<Void, UserBean> prepareJoinPropertyMeta() throws Exception,
			NoSuchMethodException
	{
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.type(PropertyType.SIMPLE)//
				.accesors() //
				.build();

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setIdMeta(joinIdMeta);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);
		joinProperties.addCascadeType(MERGE);

		Method userGetter = CompleteBean.class.getMethod("getUser");
		Method userSetter = CompleteBean.class.getMethod("setUser", UserBean.class);

		PropertyMeta<Void, UserBean> joinPropertyMeta = new PropertyMeta<Void, UserBean>();
		joinPropertyMeta.setSingleKey(true);
		joinPropertyMeta.setJoinProperties(joinProperties);
		joinPropertyMeta.setGetter(userGetter);
		joinPropertyMeta.setSetter(userSetter);

		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetaMap.put("joinEntity", joinPropertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		return joinPropertyMeta;
	}
}
