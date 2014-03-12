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

import java.io.Serializable;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

public class ProxyClassFactory {

	public Class<?> createProxyClass(Class<?> entityClass) {
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entityClass);
		enhancer.setInterfaces(new Class[]{Serializable.class});
		enhancer.setClassLoader(this.getClass().getClassLoader());
		enhancer.setUseCache(true);
		enhancer.setCallbackTypes(new Class[]{MethodInterceptor.class});
		enhancer.setUseFactory(true);
		return enhancer.createClass();
	}
}
