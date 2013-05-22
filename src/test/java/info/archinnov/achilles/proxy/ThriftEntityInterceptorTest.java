package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.proxy.wrapper.AchillesListWrapper;
import info.archinnov.achilles.proxy.wrapper.AchillesMapWrapper;
import info.archinnov.achilles.proxy.wrapper.AchillesSetWrapper;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWideMapWrapper;
import info.archinnov.achilles.proxy.wrapper.ThriftJoinWideMapWrapper;
import info.archinnov.achilles.proxy.wrapper.ThriftWideMapWrapper;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import mapping.entity.WideRowBean;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodProxy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftEntityInterceptorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityInterceptorTest
{

	@Mock
	private EntityMeta entityMeta;

	private ThriftEntityInterceptor<CompleteBean> interceptor;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> alreadyLoaded;

	private List<Method> eagerGetters = new ArrayList<Method>();

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private MethodProxy proxy;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private PropertyMeta<Void, UUID> joinPropertyMeta;

	@Mock
	private Mutator<Long> mutator;

	private ThriftPersistenceContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private Map<String, ThriftGenericEntityDao> entityDaosMap;

	@Mock
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap;

	private Long key = 452L;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private PropertyMeta<Void, Long> idMeta;

	private PropertyMeta<Void, Long> joinIdMeta;

	private PropertyMeta<Void, String> nameMeta;

	private PropertyMeta<Void, UserBean> userMeta;

	@Before
	public void setUp() throws Exception
	{
		idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(SIMPLE)
				.accessors()
				.build();

		nameMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)
				.field("name")
				.type(SIMPLE)
				.accessors()
				.build();

		userMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(JOIN_SIMPLE)
				.accessors()
				.build();

		joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class)
				.field("userId")
				.type(SIMPLE)
				.accessors()
				.build();
		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
		entityMeta.setGetterMetas(getterMetas);
		entityMeta.setSetterMetas(setterMetas);
		entityMeta.setWideRow(false);
		entityMeta.setEagerGetters(eagerGetters);

		context = ThriftPersistenceContextTestBuilder //
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity)
				.entityDao(entityDao)
				.entityDaosMap(entityDaosMap)
				.columnFamilyDaosMap(columnFamilyDaosMap)
				.build();

		interceptor = ThriftEntityInterceptorBuilder.builder(context, entity).build();
		Whitebox.setInternalState(interceptor, "alreadyLoaded", alreadyLoaded);

		interceptor.setKey(key);
		Whitebox.setInternalState(interceptor, "loader", loader);
		interceptor.setDirtyMap(dirtyMap);
		interceptor.setContext(context);
		when(entityDaosMap.get("join_cf")).thenReturn(entityDao);
	}

	@Test
	public void should_get_id_value_directly() throws Throwable
	{
		Object key = this.interceptor.intercept(entity, idMeta.getGetter(), (Object[]) null, proxy);
		assertThat(key).isEqualTo(key);
	}

	@Test(expected = IllegalAccessException.class)
	public void should_exception_when_setter_called_on_id() throws Throwable
	{
		this.interceptor.intercept(entity, idMeta.getSetter(), new Object[]
		{
			1L
		}, proxy);
	}

	@Test
	public void should_get_unmapped_property() throws Throwable
	{
		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");
		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isEqualTo("name");

		verify(getterMetas).containsKey(nameMeta.getGetter());
		verify(setterMetas).containsKey(nameMeta.getGetter());
	}

	@Test
	public void should_load_lazy_property() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);
		when(alreadyLoaded.contains(nameMeta.getGetter())).thenReturn(false);
		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isEqualTo("name");

		verify(loader).loadPropertyIntoObject(entity, key, context, propertyMeta);
		verify(alreadyLoaded).add(nameMeta.getGetter());
	}

	@Test
	public void should_return_already_loaded_lazy_property() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);

		when(alreadyLoaded.contains(nameMeta.getGetter())).thenReturn(true);

		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isEqualTo("name");

		verifyZeroInteractions(loader);
		verify(alreadyLoaded, never()).add(nameMeta.getGetter());
	}

	@Test
	public void should_set_property() throws Throwable
	{
		when(setterMetas.containsKey(nameMeta.getSetter())).thenReturn(true);
		when(setterMetas.get(nameMeta.getSetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SIMPLE);

		Object[] args = new Object[]
		{
			"sdfsdvdqfv"
		};

		when(proxy.invoke(entity, args)).thenReturn(null);
		Object name = this.interceptor.intercept(entity, nameMeta.getSetter(), args, proxy);

		assertThat(name).isNull();

		verify(proxy).invoke(entity, args);
		verify(dirtyMap).put(nameMeta.getSetter(), propertyMeta);
	}

	@Test
	public void should_create_simple_join_wrapper() throws Throwable
	{
		UserBean user = new UserBean();
		user.setUserId(123L);
		EntityMeta joinEntityMeta = new EntityMeta();
		joinEntityMeta.setIdMeta(joinIdMeta);
		joinEntityMeta.setGetterMetas(getterMetas);
		joinEntityMeta.setSetterMetas(setterMetas);
		joinEntityMeta.setTableName("join_cf");
		joinEntityMeta.setEagerGetters(eagerGetters);

		when(getterMetas.containsKey(userMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(userMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SIMPLE);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when(proxy.invoke(entity, null)).thenReturn(user);

		Object actual = this.interceptor.intercept(entity, userMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(actual).isInstanceOf(Factory.class);
		assertThat(actual).isInstanceOf(UserBean.class);
	}

	@Test
	public void should_return_null_when_no_join_simple() throws Throwable
	{
		UserBean user = new UserBean();
		user.setUserId(123L);
		EntityMeta joinEntityMeta = new EntityMeta();
		joinEntityMeta.setIdMeta(joinIdMeta);
		joinEntityMeta.setGetterMetas(getterMetas);
		joinEntityMeta.setSetterMetas(setterMetas);
		joinEntityMeta.setTableName("join_cf");

		when(getterMetas.containsKey(userMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(userMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SIMPLE);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when(proxy.invoke(entity, null)).thenReturn(null);
		Object actual = this.interceptor.intercept(entity, userMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_list_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		when(proxy.invoke(entity, null)).thenReturn(Arrays.asList("a"));

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isInstanceOf(AchillesListWrapper.class);
	}

	@Test
	public void should_return_null_when_no_list() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_set_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		when(proxy.invoke(entity, null)).thenReturn(new HashSet<String>());

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isInstanceOf(AchillesSetWrapper.class);
	}

	@Test
	public void should_return_null_when_no_set() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_map_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);

		when(proxy.invoke(entity, null)).thenReturn(new HashMap<Integer, String>());

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(name).isInstanceOf(AchillesMapWrapper.class);
	}

	@Test
	public void should_return_null_when_no_map() throws Throwable
	{
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null,
				proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_widemap_wrapper() throws Throwable
	{
		WideRowBean bean = new WideRowBean();
		Method mapGetter = WideRowBean.class.getDeclaredMethod("getMap");

		when(getterMetas.containsKey(mapGetter)).thenReturn(true);
		when(getterMetas.get(mapGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(WIDE_MAP);

		Object wideMapWrapper = this.interceptor.intercept(bean, mapGetter, (Object[]) null, proxy);

		assertThat(wideMapWrapper).isInstanceOf(ThriftWideMapWrapper.class);
		assertThat(((ThriftWideMapWrapper) wideMapWrapper).getInterceptor()).isSameAs(interceptor);
	}

	@Test
	public void should_create_counter_wide_map_wrapper() throws Throwable
	{
		CompleteBean bean = new CompleteBean();
		Method popularTopicsGetter = CompleteBean.class.getDeclaredMethod("getPopularTopics");

		when(getterMetas.containsKey(popularTopicsGetter)).thenReturn(true);
		when(getterMetas.get(popularTopicsGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(COUNTER_WIDE_MAP);

		Object counterWideMapWrapper = this.interceptor.intercept(bean, popularTopicsGetter,
				(Object[]) null, proxy);

		assertThat(counterWideMapWrapper).isInstanceOf(ThriftCounterWideMapWrapper.class);
		assertThat(((ThriftCounterWideMapWrapper) counterWideMapWrapper).getInterceptor())
				.isSameAs(interceptor);
	}

	@Test
	public void should_create_wide_row_wrapper() throws Throwable
	{
		WideRowBean bean = new WideRowBean();
		Method mapGetter = WideRowBean.class.getDeclaredMethod("getMap");

		when(getterMetas.containsKey(mapGetter)).thenReturn(true);
		when(getterMetas.get(mapGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(WIDE_MAP);
		entityMeta.setWideRow(true);

		Object name = this.interceptor.intercept(bean, mapGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(ThriftWideMapWrapper.class);
		assertThat(((ThriftWideMapWrapper) name).getInterceptor()).isSameAs(interceptor);
	}

	@Test
	public void should_create_join_widemap_wrapper() throws Throwable
	{
		WideRowBean bean = new WideRowBean();
		Method mapGetter = WideRowBean.class.getDeclaredMethod("getMap");

		when(getterMetas.containsKey(mapGetter)).thenReturn(true);
		when(getterMetas.get(mapGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(JOIN_WIDE_MAP);

		Object name = this.interceptor.intercept(bean, mapGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(ThriftJoinWideMapWrapper.class);
		assertThat(((ThriftJoinWideMapWrapper) name).getInterceptor()).isSameAs(interceptor);
	}

	@Test
	public void should_create_wide_map_wrapper() throws Throwable
	{
		CompleteBean bean = new CompleteBean();
		Method externalWideMapGetter = CompleteBean.class.getDeclaredMethod("getGeoPositions");
		ThriftGenericWideRowDao externalWideMapDao = mock(ThriftGenericWideRowDao.class);

		when(getterMetas.containsKey(externalWideMapGetter)).thenReturn(true);
		when(getterMetas.get(externalWideMapGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(WIDE_MAP);
		when(propertyMeta.getExternalCFName()).thenReturn("geo_positions");
		when(propertyMeta.getIdClass()).thenReturn(Long.class);

		when(columnFamilyDaosMap.get("geo_positions")).thenReturn(externalWideMapDao);

		Object externalWideMap = this.interceptor.intercept(bean, externalWideMapGetter,
				(Object[]) null, proxy);

		assertThat(externalWideMap).isInstanceOf(ThriftWideMapWrapper.class);
		Object dao = Whitebox.getInternalState(externalWideMap, "dao");

		assertThat(dao).isNotNull();
		assertThat(dao).isSameAs(externalWideMapDao);
		assertThat(((ThriftWideMapWrapper) externalWideMap).getInterceptor()).isSameAs(interceptor);
	}

	@Test
	public void should_create_join_wide_map_wrapper() throws Throwable
	{
		CompleteBean bean = new CompleteBean();
		Method joinUsersGetter = CompleteBean.class.getDeclaredMethod("getJoinUsers");

		when(getterMetas.containsKey(joinUsersGetter)).thenReturn(true);
		when(getterMetas.get(joinUsersGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(JOIN_WIDE_MAP);
		when(propertyMeta.getExternalCFName()).thenReturn("join_users");
		when(propertyMeta.getIdClass()).thenReturn(Long.class);

		Object name = this.interceptor.intercept(bean, joinUsersGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(ThriftJoinWideMapWrapper.class);
		assertThat(((ThriftJoinWideMapWrapper) name).getInterceptor()).isSameAs(interceptor);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_call_setter_on_wide_map() throws Throwable
	{
		when(setterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(setterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.WIDE_MAP);

		this.interceptor.intercept(entity, nameMeta.getGetter(), (Object[]) null, proxy);
	}

}
