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
import net.sf.cglib.proxy.CallbackFilter;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.NoOp;

import java.io.Serializable;
import java.lang.reflect.Method;

public class ProxyClassFactory {

    private static final int NO_OP_CALLBACK_INDEX = 1;
    private static final int METHOD_INTERCEPTOR_CALLBACK_INDEX = 0;
    public static final Class[] CALLBACK_TYPES = new Class[] { MethodInterceptor.class, NoOp.class };
    public static final CallbackFilter INTERCEPT_ALL_BUT_FINALIZE = new CallbackFilter() {
        @Override
        public int accept(Method method) {
            return isFinalizeMethod(method) ? NO_OP_CALLBACK_INDEX : METHOD_INTERCEPTOR_CALLBACK_INDEX;
        }

        private boolean isFinalizeMethod(Method method) {
            return method.getName().equals("finalize")
                    && method.getReturnType().equals(Void.TYPE)
                    && method.getParameterTypes().length == 0
                    ;
        }
    };

    public Class<?> createProxyClass(Class<?> entityClass, ConfigurationContext configContext) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(entityClass);
        enhancer.setInterfaces(new Class[] { Serializable.class });
        enhancer.setClassLoader(configContext.selectClassLoader(entityClass));
        enhancer.setUseCache(true);
        enhancer.setCallbackTypes(CALLBACK_TYPES);
        enhancer.setCallbackFilter(INTERCEPT_ALL_BUT_FINALIZE);
        enhancer.setUseFactory(true);
        return enhancer.createClass();
    }

    public static enum Singleton {
        INSTANCE;

        private final ProxyClassFactory instance = new ProxyClassFactory();

        public ProxyClassFactory get() {
            return instance;
        }
    }
}
