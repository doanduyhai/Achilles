/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.persistence.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.internal.proxy.AchillesProxyInterceptor;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.ProxyClassFactory;
import info.archinnov.achilles.internal.reflection.ObjectInstantiator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.NoOp;

@RunWith(MockitoJUnitRunner.class)
public class EntityProxifierTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EntityProxifier proxifier;

    @Mock
    private ObjectInstantiator instantiator;

    @Mock
    private ProxyClassFactory factory;

    @Mock
    private ProxyInterceptor<CompleteBean> interceptor;

    @Mock
    private PersistenceContext.EntityFacade context;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private PropertyMeta idMeta;

    @Test
    public void should_derive_base_class_from_transient() throws Exception {
        assertThat(proxifier.<CompleteBean>deriveBaseClass(new CompleteBean())).isEqualTo(CompleteBean.class);
    }

    @Test
    public void should_derive_base_class() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(entity.getClass());
        enhancer.setCallback(interceptor);

        when(interceptor.getTarget()).thenReturn(entity);

        CompleteBean proxy = (CompleteBean) enhancer.create();
        assertThat(proxifier.<CompleteBean>deriveBaseClass(proxy)).isEqualTo(CompleteBean.class);
    }

    @Test
    public void should_build_proxy_with_all_fields_loaded() throws Exception {

        long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        PropertyMeta pm = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        Object value = new Object();

        CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).name("name").buid();
        proxifier = spy(proxifier);

        doReturn(interceptor).when(proxifier).buildInterceptor(eq(context), eq(entity), anySetOf(Method.class));
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.getAllMetasExceptCounters()).thenReturn(Arrays.asList(pm));
        when(entityMeta.getAllCounterMetas()).thenReturn(Arrays.asList(counterMeta));
        when(pm.forValues().getValueFromField(entity)).thenReturn(value);
        when(context.getConfigContext()).thenReturn(configContext);
        when(factory.createProxyClass(entity.getClass(), configContext)).thenReturn((Class) entity.getClass());
        when(instantiator.instantiate(Mockito.<Class<Factory>>any())).thenReturn(realProxy);

        Object proxy = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context);

        assertThat(proxy).isNotNull();
        assertThat(proxy).isInstanceOf(Factory.class);
        Factory factory = (Factory) proxy;

        assertThat(factory.getCallbacks()).hasSize(1);
        assertThat(factory.getCallback(0)).isInstanceOf(ProxyInterceptor.class);

        verify(pm.forValues()).getValueFromField(entity);
        verify(pm.forValues()).setValueToField(realProxy, value);
        verify(counterMeta.forValues()).setValueToField(entity,null);
    }

    @Test
    public void should_build_null_proxy() throws Exception {
        assertThat(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(null, context)).isNull();
    }

    @Test
    public void should_get_real_object_from_proxy() throws Exception {
        UserBean realObject = new UserBean();
        when(interceptor.getTarget()).thenReturn(realObject);

        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(UserBean.class);
        enhancer.setCallback(interceptor);
        UserBean proxy = (UserBean) enhancer.create();

        UserBean actual = proxifier.getRealObject(proxy);

        assertThat(actual).isSameAs(realObject);
    }

    @Test
    public void should_return_object_when_get_real_object_called_on_non_proxified_entity() throws Exception {
        UserBean realObject = new UserBean();

        UserBean actual = proxifier.getRealObject(realObject);
        assertThat(actual).isSameAs(realObject);

    }

    @Test
    public void should_proxy_true() throws Exception {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(CompleteBean.class);
        enhancer.setCallback(NoOp.INSTANCE);

        CompleteBean proxy = (CompleteBean) enhancer.create();

        assertThat(proxifier.isProxy(proxy)).isTrue();
    }

    @Test
    public void should_proxy_false() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
        assertThat(proxifier.isProxy(bean)).isFalse();
    }

    @Test
    public void should_get_interceptor_from_proxy() throws Exception {

        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(CompleteBean.class);
        enhancer.setCallback(interceptor);
        CompleteBean proxy = (CompleteBean) enhancer.create();

        AchillesProxyInterceptor<CompleteBean> actual = proxifier.getInterceptor(proxy);

        assertThat(actual).isSameAs(interceptor);
    }

    @Test
    public void should_ensure_proxy() throws Exception {
        proxifier.ensureProxy(realProxy);
    }

    @Test
    public void should_exception_when_not_proxy() throws Exception {
        CompleteBean proxy = new CompleteBean();

        exception.expect(IllegalStateException.class);
        exception.expectMessage("The entity '" + proxy + "' is not in 'managed' state.");
        proxifier.ensureProxy(proxy);
    }

    @Test
    public void should_ensure_not_proxy() throws Exception {
        proxifier.ensureNotProxy(new CompleteBean());
    }

    @Test
    public void should_exception_when_proxy() throws Exception {

        exception.expect(IllegalStateException.class);
        exception.expectMessage("The entity is already in 'managed' state");
        proxifier.ensureNotProxy(realProxy);
    }

    @Test
    public void should_return_null_when_unproxying_null() throws Exception {
        assertThat(proxifier.removeProxy((Object) null)).isNull();
    }

    @Test
    public void should_return_same_entity_when_calling_unproxy_on_non_proxified_entity() throws Exception {
        CompleteBean realObject = new CompleteBean();

        CompleteBean actual = proxifier.removeProxy(realObject);

        assertThat(actual).isSameAs(realObject);
    }

    @Test
    public void should_unproxy_entity() throws Exception {
        when(interceptor.getTarget()).thenReturn(realProxy);

        Factory actual = proxifier.removeProxy(realProxy);

        assertThat(actual).isSameAs(realProxy);
    }

    @Test
    public void should_unproxy_real_entryset() throws Exception {
        Map<Integer, CompleteBean> map = new HashMap<>();
        CompleteBean completeBean = new CompleteBean();
        map.put(1, completeBean);
        Map.Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

        when(proxifier.isProxy(completeBean)).thenReturn(false);

        Map.Entry<Integer, CompleteBean> actual = proxifier.removeProxy(entry);
        assertThat(actual).isSameAs(entry);
        assertThat(actual.getValue()).isSameAs(completeBean);
    }

    @Test
    public void should_unproxy_entryset_containing_proxy() throws Exception {
        Map<Integer, Factory> map = new HashMap<>();
        map.put(1, realProxy);
        Map.Entry<Integer, Factory> entry = map.entrySet().iterator().next();

        when(interceptor.getTarget()).thenReturn(realProxy);

        Map.Entry<Integer, Factory> actual = proxifier.removeProxy(entry);
        assertThat(actual).isSameAs(entry);
        assertThat(actual.getValue()).isSameAs(realProxy);
    }

    @Test
    public void should_unproxy_collection_of_entities() throws Exception {
        Collection<Factory> proxies = new ArrayList<>();
        proxies.add(realProxy);

        when(interceptor.getTarget()).thenReturn(realProxy);

        Collection<Factory> actual = proxifier.removeProxy(proxies);

        assertThat(actual).containsExactly(realProxy);
    }

    @Test
    public void should_unproxy_list_of_entities() throws Exception {
        List<Factory> proxies = new ArrayList<>();
        proxies.add(realProxy);

        when(interceptor.getTarget()).thenReturn(realProxy);

        Collection<Factory> actual = proxifier.removeProxy(proxies);

        assertThat(actual).containsExactly(realProxy);
    }

    @Test
    public void should_unproxy_set_of_entities() throws Exception {
        Set<Factory> proxies = new HashSet<>();
        proxies.add(realProxy);

        when(interceptor.getTarget()).thenReturn(realProxy);

        Collection<Factory> actual = proxifier.removeProxy(proxies);

        assertThat(actual).containsExactly(realProxy);
    }

    private Factory realProxy = new Factory() {

        @Override
        public Object newInstance(Callback callback) {
            return null;
        }

        @Override
        public Object newInstance(Callback[] callbacks) {
            return null;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Object newInstance(Class[] types, Object[] args, Callback[] callbacks) {
            return null;
        }

        @Override
        public Callback getCallback(int index) {
            return interceptor;
        }

        @Override
        public void setCallback(int index, Callback callback) {

        }

        @Override
        public void setCallbacks(Callback[] callbacks) {

        }

        @Override
        public Callback[] getCallbacks() {
            return new Callback[] { interceptor };
        }

    };
}
