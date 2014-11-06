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

package info.archinnov.achilles.internal.proxy;

import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Factory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyClassFactoryTest {

    private ProxyClassFactory factory = new ProxyClassFactory();

    @Mock
    private ConfigurationContext configContext;

    @Test
    public void should_create_proxy_class() throws Exception {
        //When
        when(configContext.selectClassLoader(CompleteBean.class)).thenReturn(CompleteBean.class.getClassLoader());
        Class<?> proxyClass = factory.createProxyClass(CompleteBean.class, configContext);

        //Then
        assertThat(CompleteBean.class.isAssignableFrom(proxyClass)).isTrue();
        assertThat(Factory.class.isAssignableFrom(proxyClass)).isTrue();
    }

    @Test
    public void should_reuse_created_proxy_class_from_cache() throws Exception {
        //When
        when(configContext.selectClassLoader(CompleteBean.class)).thenReturn(CompleteBean.class.getClassLoader());
        Class<?> proxyClass1 = factory.createProxyClass(CompleteBean.class, configContext);
        Class<?> proxyClass2 = factory.createProxyClass(CompleteBean.class, configContext);

        //Then
        assertThat(proxyClass1 == proxyClass2).isTrue();
    }

    @Test
    public void should_not_intercept_finalize_methods() throws Exception {
        Class<?> proxyClass = factory.createProxyClass(CompleteBean.class, new ConfigurationContext());

        try {
            proxyClass.getDeclaredMethod("finalize");
            fail("Should have reported the finalize method don't exists");
        } catch (NoSuchMethodException nsm) {
            assertThat(nsm).hasMessageContaining("finalize");
        }
    }
}
