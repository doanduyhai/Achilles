package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.proxy.builder.EntityProxyBuilder;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;
import integration.tests.entity.User;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.FlushModeType;

import mapping.entity.CompleteBean;
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
	private EntityProxyBuilder interceptorBuilder;

	@Mock
	private EntityHelper helper;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private Mutator<Long> mutator;

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
		Whitebox.setInternalState(em, "interceptorBuilder", interceptorBuilder);

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
		em.persist(entity);

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
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));

		em.remove(entity);
		verify(persister).remove(entity, entityMeta);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_removing_unmanaged_entity() throws Exception
	{
		when((Class<CompleteBean>) helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(helper.isProxy(entity)).thenReturn(false);

		em.remove(entity);
	}

	@Test
	public void should_find() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

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

		when(helper.isProxy(bean)).thenReturn(true);
		when(helper.deriveBaseClass(bean)).thenReturn((Class) CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		GenericDynamicCompositeDao<Long> entityDao = mock(GenericDynamicCompositeDao.class);
		when(entityMeta.getEntityDao()).thenReturn(entityDao);

		JoinProperties joinProperties = new JoinProperties();
		EntityMeta<Integer> joinMeta = new EntityMeta<Integer>();
		GenericDynamicCompositeDao<Integer> joinDao = mock(GenericDynamicCompositeDao.class);
		joinMeta.setEntityDao(joinDao);
		joinProperties.setEntityMeta(joinMeta);
		Mutator<Integer> joinMutator = mock(Mutator.class);

		PropertyMeta<Long, User> propertyMeta = new PropertyMeta<Long, User>();
		propertyMeta.setType(JOIN_WIDE_MAP);
		propertyMeta.setPropertyName("users");
		propertyMeta.setJoinProperties(joinProperties);

		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetaMap.put("users", propertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);
		when(entityDao.buildMutator()).thenReturn(mutator);

		JpaEntityInterceptor<Long> interceptor = mock(JpaEntityInterceptor.class);

		when(bean.getCallback(0)).thenReturn(interceptor);
		when(joinDao.buildMutator()).thenReturn(joinMutator);

		em.startBatch(bean);

		verify(interceptor).setMutator(mutator);
		verify(interceptor).setMutatorMap(mutatorMapCaptor.capture());

		assertThat(mutatorMapCaptor.getValue().get("users")).isSameAs((Mutator) joinMutator);

	}

	@Test
	public void should_exception_when_trying_to_batch_transient_entity() throws Exception
	{
		Factory bean = mock(Factory.class);
		when(helper.isProxy(bean)).thenReturn(false);

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("The entity is not in 'managed' state. Please merge it before starting a batch");

		em.startBatch(bean);
	}

	@Test
	public void should_end_batch() throws Exception
	{
		Factory bean = mock(Factory.class);
		JpaEntityInterceptor<Long> interceptor = mock(JpaEntityInterceptor.class);

		when(helper.isProxy(bean)).thenReturn(true);
		when(bean.getCallback(0)).thenReturn(interceptor);
		when(interceptor.getMutator()).thenReturn(mutator);

		Map<String, Mutator<?>> mutatorMap = new HashMap<String, Mutator<?>>();
		Mutator<Integer> joinMutator = mock(Mutator.class);
		mutatorMap.put("test", joinMutator);

		when(interceptor.getMutatorMap()).thenReturn(mutatorMap);

		em.endBatch(bean);

		verify(mutator).execute();
		verify(joinMutator).execute();
	}

	@Test
	public void should_exception_when_trying_to_end_batch_on_transient_entity() throws Exception
	{
		Factory bean = mock(Factory.class);

		when(helper.isProxy(bean)).thenReturn(false);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("The entity is not in 'managed' state");

		em.endBatch(bean);

	}
}
