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

package info.archinnov.achilles.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Factory;

@RunWith(MockitoJUnitRunner.class)
public class ProxyClassFactoryTest {

	private ProxyClassFactory factory = new ProxyClassFactory();

	@Test
	public void should_create_proxy_class() throws Exception {
		//When
		Class<?> proxyClass = factory.createProxyClass(CompleteBean.class);

		//Then
		assertThat(CompleteBean.class.isAssignableFrom(proxyClass)).isTrue();
		assertThat(Factory.class.isAssignableFrom(proxyClass)).isTrue();
	}

	@Test
	public void should_reuse_created_proxy_class_from_cache() throws Exception {
		//When
		Class<?> proxyClass1 = factory.createProxyClass(CompleteBean.class);
		Class<?> proxyClass2 = factory.createProxyClass(CompleteBean.class);

		//Then
		assertThat(proxyClass1 == proxyClass2).isTrue();
	}
}
