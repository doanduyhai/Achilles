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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;

public class EntityRefresher {
    private static final Logger log = LoggerFactory.getLogger(EntityRefresher.class);

    private EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    private EntityLoader loader = EntityLoader.Singleton.INSTANCE.get();
    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    public <T> AchillesFuture<T> refresh(T proxy, final EntityOperations context) throws AchillesStaleObjectStateException {
        final Object primaryKey = context.getPrimaryKey();
        log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
                primaryKey);

        final ProxyInterceptor<T> interceptor = proxifier.getInterceptor(proxy);
        final Object entity = context.getEntity();

        interceptor.getDirtyMap().clear();

        final Class<T> entityClass = context.getEntityClass();
        final AchillesFuture<T> entityFuture = loader.load(context, entityClass);

        Function<T, T> updateInterceptor = updateProxyInterceptor(context, interceptor, entity, primaryKey);
        final ListenableFuture<T> triggerInterceptors = asyncUtils.transformFutureSync(entityFuture, updateInterceptor);
        return asyncUtils.buildInterruptible(triggerInterceptors);
    }

    protected <T> Function<T, T> updateProxyInterceptor(final EntityOperations context, final ProxyInterceptor<T> interceptor, final Object entity, final Object primaryKey) {
        return new Function<T, T>() {
                @Override
                public T apply(T freshEntity) {
                    if (freshEntity == null) {
                        throw new AchillesStaleObjectStateException("The entity '" + entity + "' with primary_key '" + primaryKey + "' no longer exists in Cassandra");
                    }
                    interceptor.setTarget(freshEntity);
                    interceptor.getAlreadyLoaded().clear();
                    interceptor.getAlreadyLoaded().addAll(context.getAllGettersExceptCounters());
                    return freshEntity;
                }
            };
    }

    public static enum Singleton {
        INSTANCE;

        private final EntityRefresher instance = new EntityRefresher();

        public EntityRefresher get() {
            return instance;
        }
    }
}
