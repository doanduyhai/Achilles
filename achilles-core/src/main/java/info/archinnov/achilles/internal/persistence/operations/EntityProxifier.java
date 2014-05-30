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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.EntityInterceptor;
import info.archinnov.achilles.internal.proxy.EntityInterceptorBuilder;
import info.archinnov.achilles.internal.proxy.ProxyClassFactory;
import info.archinnov.achilles.internal.reflection.ObjectInstantiator;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Factory;

public class EntityProxifier {

    private static final Logger log = LoggerFactory.getLogger(EntityProxifier.class);

    private ObjectInstantiator instantiator = new ObjectInstantiator();

    private ProxyClassFactory factory = new ProxyClassFactory();

    @SuppressWarnings("unchecked")
    public <T> Class<T> deriveBaseClass(Object entity) {
        log.debug("Deriving base class for entity {} ", entity);

        Class<T> baseClass = (Class<T>) entity.getClass();
        if (isProxy(entity)) {
            EntityInterceptor<?> interceptor = getInterceptor(entity);
            baseClass = (Class<T>) interceptor.getTarget().getClass();
        }

        return baseClass;
    }

    public <T> T buildProxyWithAllFieldsLoadedExceptCounters(T entity, EntityOperations context) {
        return buildProxy(entity, context, context.getAllGettersExceptCounters());
    }

    public <T> T buildProxyWithNoFieldLoaded(T entity, EntityOperations context) {
        return buildProxy(entity, context, new HashSet<Method>());
    }

    public <T> T buildProxy(T entity, EntityOperations context, Set<Method> alreadyLoaded) {

        if (entity == null) {
            return null;
        }

        log.debug("Build Cglib proxy for entity {} ", entity);

        Class<?> proxyClass = factory.createProxyClass(entity.getClass());

        @SuppressWarnings("unchecked")
        T instance = (T) instantiator.instantiate(proxyClass);

        EntityMeta meta = context.getEntityMeta();
        for (PropertyMeta pm : meta.getAllMetas()) {
            Object value = pm.getValueFromField(entity);
            pm.setValueToField(instance, value);
        }

        ((Factory) instance).setCallbacks(new Callback[] { buildInterceptor(context, entity, alreadyLoaded) });
        return instance;
    }

    @SuppressWarnings("unchecked")
    public <T> T getRealObject(T proxy) {
        log.debug("Get real entity from proxy {} ", proxy);

        if (isProxy(proxy)) {
            Factory factory = (Factory) proxy;
            EntityInterceptor<T> interceptor = (EntityInterceptor<T>) factory.getCallback(0);
            return (T) interceptor.getTarget();
        } else {
            return proxy;
        }

    }

    public boolean isProxy(Object entity) {
        return Factory.class.isAssignableFrom(entity.getClass());
    }

    public <T> EntityInterceptor<T> getInterceptor(T proxy) {
        log.debug("Get interceptor from proxy {} ", proxy);

        Factory factory = (Factory) proxy;

        @SuppressWarnings("unchecked")
        EntityInterceptor<T> interceptor = (EntityInterceptor<T>) factory.getCallback(0);
        return interceptor;
    }

    public void ensureProxy(Object proxy) {
        if (!isProxy(proxy)) {
            throw new IllegalStateException("The entity '" + proxy + "' is not in 'managed' state.");
        }
    }

    public void ensureNotProxy(Object rawEntity) {
        if (isProxy(rawEntity)) {
            throw new IllegalStateException("Then entity is already in 'managed' state.");
        }
    }

    public <T> T removeProxy(T proxy) {
        log.debug("Unwrapping object {} ", proxy);

        if (proxy != null) {

            if (isProxy(proxy)) {
                return getRealObject(proxy);
            } else {
                return proxy;
            }
        } else {
            return null;
        }
    }

    public <K, V> Map.Entry<K, V> removeProxy(Map.Entry<K, V> entry) {
        V value = entry.getValue();
        if (isProxy(value)) {
            value = getRealObject(value);
            entry.setValue(value);
        }
        return entry;
    }

    public <T> Collection<T> removeProxy(Collection<T> proxies) {
        Collection<T> result = new ArrayList<>();
        for (T proxy : proxies) {
            result.add(removeProxy(proxy));
        }
        return result;
    }

    public <T> List<T> removeProxy(List<T> proxies) {
        List<T> result = new ArrayList<>();
        for (T proxy : proxies) {
            result.add(this.removeProxy(proxy));
        }

        return result;
    }

    public <T> Set<T> removeProxy(Set<T> proxies) {
        Set<T> result = new HashSet<>();
        for (T proxy : proxies) {
            result.add(this.removeProxy(proxy));
        }

        return result;
    }

    public <T> EntityInterceptor<T> buildInterceptor(EntityOperations context, T entity, Set<Method> alreadyLoaded) {
        return new EntityInterceptorBuilder<>(context, entity).alreadyLoaded(alreadyLoaded).build();
    }

}
