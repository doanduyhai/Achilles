/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.NoOp;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityProxifierTest {
	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private EntityInterceptor<PersistenceContext, CompleteBean> interceptor;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PropertyMeta idMeta;

	@Before
	public void setUp() {
		doCallRealMethod().when(proxifier).deriveBaseClass(any());
	}

	@Test
	public void should_derive_base_class_from_transient() throws Exception {
		assertThat(
				(Class<CompleteBean>) proxifier
						.deriveBaseClass(new CompleteBean())).isEqualTo(
				CompleteBean.class);
	}

	@Test
	public void should_derive_base_class() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());
		enhancer.setCallback(interceptor);

		when(interceptor.getTarget()).thenReturn(entity);

		doCallRealMethod().when(proxifier).isProxy(any());
		doCallRealMethod().when(proxifier).getInterceptor(any());

		CompleteBean proxy = (CompleteBean) enhancer.create();
		assertThat((Class<CompleteBean>) proxifier.deriveBaseClass(proxy))
				.isEqualTo(CompleteBean.class);
	}

	@Test
	public void should_build_proxy() throws Exception {

		long primaryKey = 1L;

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey)
				.name("name").buid();

		when((PropertyMeta) entityMeta.getIdMeta()).thenReturn(idMeta);

		when(
				proxifier.buildInterceptor(eq(context), eq(entity),
						any(HashSet.class))).thenReturn(interceptor);

		doCallRealMethod().when(proxifier).buildProxy(entity, context);
		doCallRealMethod().when(proxifier).buildProxy(eq(entity), eq(context),
				any(HashSet.class));

		CompleteBean proxy = proxifier.buildProxy(entity, context);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0))
				.isInstanceOf(EntityInterceptor.class);
	}

	@Test
	public void should_build_null_proxy() throws Exception {
		doCallRealMethod().when(proxifier).buildProxy(null, context);
		assertThat(proxifier.buildProxy(null, context)).isNull();
	}

	@Test
	public void should_get_real_object_from_proxy() throws Exception {
		UserBean realObject = new UserBean();
		when(interceptor.getTarget()).thenReturn(realObject);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		doCallRealMethod().when(proxifier).isProxy(any());
		doCallRealMethod().when(proxifier).getRealObject(any());
		UserBean actual = proxifier.getRealObject(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_return_object_when_get_real_object_called_on_non_proxified_entity()
			throws Exception {
		UserBean realObject = new UserBean();
		doCallRealMethod().when(proxifier).isProxy(realObject);
		doCallRealMethod().when(proxifier).getRealObject(realObject);

		UserBean actual = proxifier.getRealObject(realObject);
		assertThat(actual).isSameAs(realObject);

	}

	@Test
	public void should_proxy_true() throws Exception {
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		doCallRealMethod().when(proxifier).isProxy(any());
		assertThat(proxifier.isProxy(proxy)).isTrue();
	}

	@Test
	public void should_proxy_false() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
		doCallRealMethod().when(proxifier).isProxy(any());
		assertThat(proxifier.isProxy(bean)).isFalse();
	}

	@Test
	public void should_get_interceptor_from_proxy() throws Exception {

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
	public void should_ensure_proxy() throws Exception {
		CompleteBean proxy = new CompleteBean();

		when(proxifier.isProxy(proxy)).thenReturn(true);
		doCallRealMethod().when(proxifier).ensureProxy(proxy);
		proxifier.ensureProxy(proxy);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_not_proxy() throws Exception {
		CompleteBean proxy = new CompleteBean();

		when(proxifier.isProxy(proxy)).thenReturn(false);
		doCallRealMethod().when(proxifier).ensureProxy(proxy);
		proxifier.ensureProxy(proxy);
	}

	@Test
	public void should_return_null_when_unproxying_null() throws Exception {
		doCallRealMethod().when(proxifier).unwrap(any());
		assertThat(proxifier.unwrap((Object) null)).isNull();
	}

	@Test
	public void should_return_same_entity_when_calling_unproxy_on_non_proxified_entity()
			throws Exception {
		CompleteBean realObject = new CompleteBean();
		when(proxifier.isProxy(realObject)).thenReturn(false);
		doCallRealMethod().when(proxifier).unwrap(any());
		CompleteBean actual = proxifier.unwrap(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_entity() throws Exception {
		CompleteBean realObject = new CompleteBean();

		when(proxifier.isProxy(realObject)).thenReturn(true);
		when(proxifier.getRealObject(realObject)).thenReturn(realObject);

		doCallRealMethod().when(proxifier).unwrap(any());
		CompleteBean actual = proxifier.unwrap(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_real_entryset() throws Exception {
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		CompleteBean completeBean = new CompleteBean();
		map.put(1, completeBean);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(proxifier.isProxy(completeBean)).thenReturn(false);
		doCallRealMethod().when(proxifier).unwrap(entry);

		Entry<Integer, CompleteBean> actual = proxifier.unwrap(entry);
		assertThat(actual).isSameAs(entry);
		assertThat(actual.getValue()).isSameAs(completeBean);
	}

	@Test
	public void should_unproxy_entryset_containing_proxy() throws Exception {
		CompleteBean completeBean = new CompleteBean();
		CompleteBean realObject = new CompleteBean();
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, completeBean);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(proxifier.isProxy(completeBean)).thenReturn(true);
		when(proxifier.getRealObject(completeBean)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unwrap(entry);

		Entry<Integer, CompleteBean> actual = proxifier.unwrap(entry);
		assertThat(actual).isSameAs(entry);
		assertThat(actual.getValue()).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_collection_of_entities() throws Exception {
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();

		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unwrap(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unwrap(proxies);

		Collection<CompleteBean> actual = proxifier.unwrap(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_unproxy_list_of_entities() throws Exception {
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unwrap(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unwrap(proxies);

		Collection<CompleteBean> actual = proxifier.unwrap(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_unproxy_set_of_entities() throws Exception {
		CompleteBean realObject = new CompleteBean();
		CompleteBean proxy = new CompleteBean();
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();
		proxies.add(proxy);

		when(proxifier.unwrap(proxy)).thenReturn(realObject);
		doCallRealMethod().when(proxifier).unwrap(proxies);

		Collection<CompleteBean> actual = proxifier.unwrap(proxies);

		assertThat(actual).containsExactly(realObject);
	}

}
