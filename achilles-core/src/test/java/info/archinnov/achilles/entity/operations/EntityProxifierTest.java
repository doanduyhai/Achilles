package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.EntityInterceptor;

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
import net.sf.cglib.asm.Type;
import net.sf.cglib.core.ClassGenerator;
import net.sf.cglib.core.DefaultGeneratorStrategy;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.NoOp;
import net.sf.cglib.transform.ClassTransformer;
import net.sf.cglib.transform.TransformingClassGenerator;
import net.sf.cglib.transform.impl.InterceptFieldEnabled;
import net.sf.cglib.transform.impl.InterceptFieldFilter;
import net.sf.cglib.transform.impl.InterceptFieldTransformer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;

/**
 * AchillesEntityProxifierTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class EntityProxifierTest
{
	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private EntityInterceptor<PersistenceContext, CompleteBean> interceptor;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Before
	public void setUp()
	{
		doCallRealMethod().when(proxifier).deriveBaseClass(any());
	}

	@Test
	public void should_derive_base_class_from_transient() throws Exception
	{
		assertThat((Class<CompleteBean>) proxifier.deriveBaseClass(new CompleteBean())).isEqualTo(
				CompleteBean.class);
	}

	@Test
	public void should_derive_base_class() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());
		enhancer.setCallback(interceptor);

		when(interceptor.getTarget()).thenReturn(entity);

		doCallRealMethod().when(proxifier).isProxy(any());
		doCallRealMethod().when(proxifier).getInterceptor(any());

		CompleteBean proxy = (CompleteBean) enhancer.create();
		assertThat((Class<CompleteBean>) proxifier.deriveBaseClass(proxy)).isEqualTo(
				CompleteBean.class);
	}

	@Test
	public void should_build_proxy() throws Exception
	{

		long primaryKey = 1L;

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).name("name").buid();

		when((PropertyMeta) entityMeta.getIdMeta()).thenReturn(idMeta);

		when(proxifier.buildInterceptor(context, entity)).thenReturn(interceptor);

		doCallRealMethod().when(proxifier).buildProxy(entity, context);

		CompleteBean proxy = proxifier.buildProxy(entity, context);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0)).isInstanceOf(EntityInterceptor.class);
	}

	@Test
	public void should_build_null_proxy() throws Exception
	{
		doCallRealMethod().when(proxifier).buildProxy(null, context);
		assertThat(proxifier.buildProxy(null, context)).isNull();
	}

	@Test
	public void should_get_real_object_from_proxy() throws Exception
	{
		UserBean realObject = new UserBean();
		when(interceptor.getTarget()).thenReturn(realObject);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		doCallRealMethod().when(proxifier).getRealObject(any());
		UserBean actual = proxifier.getRealObject(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_proxy_true() throws Exception
	{
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		doCallRealMethod().when(proxifier).isProxy(any());
		assertThat(proxifier.isProxy(proxy)).isTrue();
	}

	@Test
	public void should_proxy_false() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
		doCallRealMethod().when(proxifier).isProxy(any());
		assertThat(proxifier.isProxy(bean)).isFalse();
	}

	@Test
	public void should_get_interceptor_from_proxy() throws Exception
	{

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();

		doCallRealMethod().when(proxifier).getInterceptor(any());
		EntityInterceptor<PersistenceContext, CompleteBean> actual = proxifier
				.getInterceptor(proxy);

		assertThat(actual).isSameAs(interceptor);
	}

	@Test
	public void should_ensure_proxy() throws Exception
	{
		CompleteBean proxy = new CompleteBean();

		when(proxifier.isProxy(proxy)).thenReturn(true);
		doCallRealMethod().when(proxifier).ensureProxy(proxy);
		proxifier.ensureProxy(proxy);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_not_proxy() throws Exception
	{
		CompleteBean proxy = new CompleteBean();

		when(proxifier.isProxy(proxy)).thenReturn(false);
		doCallRealMethod().when(proxifier).ensureProxy(proxy);
		proxifier.ensureProxy(proxy);
	}

	@Test
	public void should_return_null_when_unproxying_null() throws Exception
	{
		doCallRealMethod().when(proxifier).unproxy(any());
		assertThat(proxifier.unproxy((Object) null)).isNull();
	}

	@Test
	public void should_return_same_entity_when_calling_unproxy_on_non_proxified_entity()
			throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		when(proxifier.isProxy(realObject)).thenReturn(false);
		doCallRealMethod().when(proxifier).unproxy(any());
		CompleteBean actual = proxifier.unproxy(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_entity() throws Exception
	{
		CompleteBean realObject = new CompleteBean();

		when(proxifier.isProxy(realObject)).thenReturn(true);
		when(proxifier.getRealObject(realObject)).thenReturn(realObject);

		doCallRealMethod().when(proxifier).unproxy(any());
		CompleteBean actual = proxifier.unproxy(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_real_entryset() throws Exception
	{
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		CompleteBean completeBean = new CompleteBean();
		map.put(1, completeBean);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(proxifier.isProxy(completeBean)).thenReturn(false);
		doCallRealMethod().when(proxifier).unproxy(entry);

		Entry<Integer, CompleteBean> actual = proxifier.unproxy(entry);
		assertThat(actual).isSameAs(entry);
		assertThat(actual.getValue()).isSameAs(completeBean);
	}

	@Test
	public void should_unproxy_entryset_containing_proxy() throws Exception
	{
		CompleteBean completeBean = new CompleteBean();
		CompleteBean realObject = new CompleteBean();
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, completeBean);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(proxifier.isProxy(completeBean)).thenReturn(true);
		when(proxifier.getRealObject(completeBean)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unproxy(entry);

		Entry<Integer, CompleteBean> actual = proxifier.unproxy(entry);
		assertThat(actual).isSameAs(entry);
		assertThat(actual.getValue()).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_collection_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();

		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unproxy(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unproxy(proxies);

		Collection<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_unproxy_list_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unproxy(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unproxy(proxies);

		Collection<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_unproxy_set_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unproxy(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unproxy(proxies);

		Collection<CompleteBean> actual = proxifier.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_intercept_fields() throws Exception
	{
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);
		enhancer.setStrategy(new DefaultGeneratorStrategy()
		{
			protected ClassGenerator transform(ClassGenerator cg)
			{
				ClassTransformer transf = new InterceptFieldTransformer(
						new InterceptFieldFilter()
						{
							@Override
							public boolean acceptWrite(Type owner, String name)
							{
								return true;
							}

							@Override
							public boolean acceptRead(Type owner, String name)
							{
								return true;
							}
						}
						);

				return new TransformingClassGenerator(cg, transf);
			}
		});
		CompleteBean entity = (CompleteBean) enhancer.create();
		MyFieldInterceptor fieldInterceptor = new MyFieldInterceptor();
		InterceptFieldEnabled interceptFieldEnabled = (InterceptFieldEnabled) entity;
		interceptFieldEnabled.setInterceptFieldCallback(fieldInterceptor);

		assertThat(interceptFieldEnabled.getInterceptFieldCallback()).isSameAs(fieldInterceptor);
		entity.setUser(new UserBean());
	}

}
