package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.FlushModeType;
import javax.persistence.JoinColumn;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import parser.entity.Bean;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityManager em;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityMerger merger;

	@Mock
	private EntityHelper helper;

	@Mock
	private EntityValidator entityValidator;

	@Mock
	private EntityMeta entityMeta;

	private Map<String, PropertyMeta<?, ?>> propertyMetas;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private GenericDynamicCompositeDao<Long> entityDao;

	@Captor
	ArgumentCaptor<Map<String, Mutator<?>>> mutatorMapCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

	private Method idGetter;

	@Before
	public void setUp() throws Exception
	{
		Whitebox.setInternalState(em, "persister", persister);
		merger.setPersister(persister);
		Whitebox.setInternalState(em, "loader", loader);
		Whitebox.setInternalState(em, "merger", merger);
		Whitebox.setInternalState(em, "helper", helper);
		Whitebox.setInternalState(em, "entityValidator", entityValidator);

		propertyMetas = mock(Map.class);

		idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);

		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		PropertyMeta<Void, Long> propertyMeta = mock(PropertyMeta.class);
		when(entityMeta.getIdMeta()).thenReturn(propertyMeta);
		when(propertyMeta.getGetter()).thenReturn(idGetter);

	}

	@Test
	public void should_persist() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		when(helper.isProxy(entity)).thenReturn(false);
		em.persist(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(persister).persist(entity, entityMeta);

	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		when(helper.isProxy(entity)).thenReturn(true);

		em.persist(entity);
	}

	@Test
	public void should_merge() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		when(merger.mergeEntity(entity, entityMeta)).thenReturn(entity);

		CompleteBean mergedEntity = em.merge(entity);

		assertThat(mergedEntity).isSameAs(entity);
	}

	@Test
	public void should_remove() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(helper.isProxy(entity)).thenReturn(true);
		when(helper.deriveBaseClass(entity)).thenReturn((Class) CompleteBean.class);

		em.remove(entity);
		verify(persister).remove(entity, entityMeta);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_removing_unmanaged_entity() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		doThrow(new IllegalStateException()).when(helper).ensureProxy(entity);

		em.remove(entity);
	}

	@Test
	public void should_find() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(helper.buildProxy(entity, entityMeta)).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(helper.buildProxy(entity, entityMeta)).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_flush_mode() throws Exception
	{
		FlushModeType flushMode = em.getFlushMode();

		assertThat(flushMode).isEqualTo(FlushModeType.AUTO);
	}

	@Test
	public void should_refresh() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn(entityMeta);

		when(loader.load(Bean.class, 1L, entityMeta)).thenReturn(entity);

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_set_flush_mode() throws Exception
	{
		em.setFlushMode(FlushModeType.COMMIT);
	}

	@Test
	public void should_start_batch() throws Exception
	{
		Factory bean = mock(Factory.class);

		when(helper.deriveBaseClass(bean)).thenReturn((Class) CompleteBean.class);
		when(entityMeta.getEntityDao()).thenReturn(entityDao);

		GenericDynamicCompositeDao<Integer> joinDao = mock(GenericDynamicCompositeDao.class);

		EntityMeta<Integer> joinMeta = new EntityMeta<Integer>();
		joinMeta.setEntityDao(joinDao);
		Mutator<Integer> joinMutator = mock(Mutator.class);

		PropertyMeta<Long, UserBean> propertyMeta = PropertyMetaTestBuilder
				.noClass(Long.class, UserBean.class) //
				.type(JOIN_WIDE_MAP) //
				.field("users") //
				.joinMeta(joinMeta) //
				.build();

		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetaMap.put("users", propertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);
		when(entityDao.buildMutator()).thenReturn(mutator);

		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		when(bean.getCallback(0)).thenReturn(interceptor);
		when(joinDao.buildMutator()).thenReturn(joinMutator);

		em.startBatch(bean);

		verify(helper).ensureProxy(bean);
		verify(interceptor).setMutator(mutator);
		verify(interceptor).setMutatorMap(mutatorMapCaptor.capture());

		assertThat(mutatorMapCaptor.getValue().get("users")).isSameAs((Mutator) joinMutator);

	}

	@Test
	public void should_exception_when_trying_to_batch_transient_entity() throws Exception
	{
		CompleteBean completeBean = new CompleteBean();

		doThrow(new IllegalStateException("test")).when(helper).ensureProxy(completeBean);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("test");

		em.startBatch(completeBean);
	}

	@Test
	public void should_exception_when_trying_to_start_batch_with_null_entity() throws Exception
	{
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot start batch for null entity ...");
		em.startBatch(null);
	}

	@Test
	public void should_end_batch() throws Exception
	{
		CompleteBean bean = new CompleteBean();
		JpaEntityInterceptor<Object, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		when(helper.isProxy(bean)).thenReturn(true);
		when(helper.deriveBaseClass(bean)).thenReturn((Class) CompleteBean.class);
		when(helper.getInterceptor(bean)).thenReturn(interceptor);
		when(interceptor.getMutator()).thenReturn((Mutator) mutator);
		when(entityMeta.getEntityDao()).thenReturn(entityDao);

		Map<String, Mutator<?>> mutatorMap = new HashMap<String, Mutator<?>>();
		Mutator<Integer> joinMutator = mock(Mutator.class);
		mutatorMap.put("test", joinMutator);
		when(interceptor.getMutatorMap()).thenReturn(mutatorMap);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

		GenericDynamicCompositeDao<Integer> joinDao = mock(GenericDynamicCompositeDao.class);
		EntityMeta<Integer> joinMeta = new EntityMeta<Integer>();
		joinMeta.setEntityDao(joinDao);

		PropertyMeta<Void, UserBean> joinPropertyMeta = PropertyMetaTestBuilder
				.noClass(Void.class, UserBean.class) //
				.joinMeta(joinMeta) //
				.build();

		when((PropertyMeta<Void, UserBean>) propertyMetas.get("test")).thenReturn(joinPropertyMeta);

		em.endBatch(bean);

		verify(helper).ensureProxy(bean);
		verify(entityDao).executeMutator(mutator);
		verify(joinDao).executeMutator(joinMutator);
		verify(interceptor).setMutatorMap(null);
	}

	@Test
	public void should_exception_when_trying_to_end_batch_with_null_entity() throws Exception
	{
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot end batch for null entity ...");
		em.endBatch(null);
	}

	@Test
	public void should_do_nothing_when_no_batch_started() throws Exception
	{
		CompleteBean bean = new CompleteBean();
		JpaEntityInterceptor<Object, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		when(helper.getInterceptor(bean)).thenReturn(interceptor);
		when(helper.isProxy(bean)).thenReturn(true);
		when(helper.getRealObject(bean)).thenReturn(bean);
		when(interceptor.getMutator()).thenReturn(null);
		Map<String, Mutator<?>> mutatorMap = new HashMap<String, Mutator<?>>();
		mutatorMap.put("test", null);
		when(interceptor.getMutatorMap()).thenReturn(mutatorMap);

		em.endBatch(bean);

		verifyZeroInteractions(entityDao);
		verifyZeroInteractions(mutator);
	}

	@Test
	public void should_initialize_all_lazy_fields() throws Exception
	{
		final List<Integer> getFriendsCalled = new ArrayList<Integer>();
		final List<Integer> getUserCalled = new ArrayList<Integer>();
		final List<Integer> getNameCalled = new ArrayList<Integer>();
		final List<Integer> getTweetsCalled = new ArrayList<Integer>();

		@SuppressWarnings("unused")
		class TestBean
		{
			@Column
			@Lazy
			private List<String> friends;

			@JoinColumn
			private UserBean user;

			@Column
			private String name;

			@Column
			private WideMap<UUID, String> tweets;

			public List<String> getFriends()
			{
				getFriendsCalled.add(1);
				return friends;
			}

			public void setFriends(List<String> friends)
			{
				this.friends = friends;
			}

			public UserBean getUser()
			{
				getUserCalled.add(1);
				return user;
			}

			public void setUser(UserBean user)
			{
				this.user = user;
			}

			public String getName()
			{
				getNameCalled.add(1);
				return name;
			}

			public void setName(String name)
			{
				this.name = name;
			}

			public WideMap<UUID, String> getTweets()
			{
				getTweetsCalled.add(1);
				return tweets;
			}

			public void setTweets(WideMap<UUID, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		TestBean bean = new TestBean();
		when(helper.isProxy(bean)).thenReturn(true);
		when(helper.getRealObject(bean)).thenReturn(bean);
		when(entityMetaMap.get(TestBean.class)).thenReturn(entityMeta);

		PropertyMeta<Void, String> friendsMeta = PropertyMetaTestBuilder //
				.of(TestBean.class, Void.class, String.class) //
				.field("friends") //
				.accesors() //
				.type(LAZY_LIST).build();

		PropertyMeta<Void, UserBean> userMeta = PropertyMetaTestBuilder //
				.of(TestBean.class, Void.class, UserBean.class) //
				.field("user") //
				.accesors() //
				.type(JOIN_SIMPLE).build();

		PropertyMeta<Void, String> nameMeta = PropertyMetaTestBuilder //
				.of(TestBean.class, Void.class, String.class) //
				.field("name") //
				.accesors() //
				.type(SIMPLE).build();

		PropertyMeta<UUID, String> tweetsMeta = PropertyMetaTestBuilder //
				.of(TestBean.class, UUID.class, String.class) //
				.field("tweets") //
				.accesors() //
				.type(WIDE_MAP).build();

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("friends", friendsMeta);
		propertyMetas.put("user", userMeta);
		propertyMetas.put("name", nameMeta);
		propertyMetas.put("tweets", tweetsMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

		em.initialize(bean);

		assertThat(getFriendsCalled).hasSize(1);
		assertThat(getUserCalled).hasSize(1);
		assertThat(getNameCalled).isEmpty();
		assertThat(getTweetsCalled).isEmpty();
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_entity_is_not_managed() throws Exception
	{
		CompleteBean completeBean = new CompleteBean();
		doThrow(new IllegalStateException()).when(helper).ensureProxy(completeBean);
		em.initialize(completeBean);
	}

	@Test
	public void should_unproxy_entity() throws Exception
	{
		when(helper.unproxy(entity)).thenReturn(entity);

		CompleteBean actual = em.unproxy(entity);

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_unproxy_collection_of_entity() throws Exception
	{
		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();

		when(helper.unproxy(proxies)).thenReturn(proxies);

		Collection<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_list_of_entity() throws Exception
	{
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();

		when(helper.unproxy(proxies)).thenReturn(proxies);

		List<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_set_of_entity() throws Exception
	{
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		when(helper.unproxy(proxies)).thenReturn(proxies);

		Set<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

}
