package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.NoOp;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * EntityProxifierTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class EntityProxifierTest
{
	private EntityProxifier proxifier = new EntityProxifier();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@SuppressWarnings("unchecked")
	@Test
	public void should_derive_base_class() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		JpaEntityInterceptor<Long, CompleteBean> interceptor = new JpaEntityInterceptor<Long, CompleteBean>();
		interceptor.setTarget(entity);

		enhancer.setCallback(interceptor);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat((Class<CompleteBean>) proxifier.deriveBaseClass(proxy)).isEqualTo(
				CompleteBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_derive_base_class_from_transient() throws Exception
	{
		assertThat((Class<CompleteBean>) proxifier.deriveBaseClass(new CompleteBean())).isEqualTo(
				CompleteBean.class);
	}

	@Test
	public void should_proxy_true() throws Exception
	{
		Enhancer enhancer = new Enhancer();

		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat(proxifier.isProxy(proxy)).isTrue();
	}

	@Test
	public void should_proxy_false() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
		assertThat(proxifier.isProxy(bean)).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_proxy() throws Exception
	{

		long primaryKey = 1L;

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).name("name").buid();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		PersistenceContext<Long> context = PersistenceContextTestBuilder
				.mockAll(entityMeta, CompleteBean.class, primaryKey)
				.entityDao(mock(GenericDynamicCompositeDao.class)).build();

		CompleteBean proxy = proxifier.buildProxy(entity, context);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0)).isInstanceOf(JpaEntityInterceptor.class);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_null_proxy() throws Exception
	{
		PersistenceContext<Long> context = PersistenceContextTestBuilder
				.mockAll(entityMeta, CompleteBean.class, 11L)
				.entityDao(mock(GenericDynamicCompositeDao.class)).build();

		assertThat(proxifier.buildProxy(null, context)).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_real_object_from_proxy() throws Exception
	{
		UserBean realObject = new UserBean();
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);
		when(interceptor.getTarget()).thenReturn(realObject);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		UserBean actual = proxifier.getRealObject(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_interceptor_from_proxy() throws Exception
	{
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		JpaEntityInterceptor<Long, UserBean> actual = proxifier.getInterceptor(proxy);

		assertThat(actual).isSameAs(interceptor);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_ensure_proxy() throws Exception
	{
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		proxifier.ensureProxy(proxy);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_not_proxy() throws Exception
	{
		proxifier.ensureProxy(new CompleteBean());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_entity() throws Exception
	{
		CompleteBean realObject = new CompleteBean();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();

		when(interceptor.getTarget()).thenReturn(realObject);

		CompleteBean actual = proxifier.unproxy(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_return_same_entity_when_calling_unproxy_on_non_proxified_entity()
			throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		CompleteBean actual = proxifier.unproxy(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_real_entryset() throws Exception
	{
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, new CompleteBean());
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		assertThat(proxifier.unproxy(entry)).isSameAs(entry);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_entryset_containing_proxy() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();

		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, proxy);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(interceptor.getTarget()).thenReturn(realObject);

		assertThat(proxifier.unproxy(entry).getValue()).isSameAs(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_collection_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		Collection<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_list_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		List<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_set_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		Set<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

}
