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

import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.context.facade.EntityOperations;

public class EntityRefresher {
    private static final Logger log = LoggerFactory.getLogger(EntityRefresher.class);

    private EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    private EntityLoader loader = EntityLoader.Singleton.INSTANCE.get();

    public void refresh(Object proxifiedEntity, EntityOperations context) throws AchillesStaleObjectStateException {
        Object primaryKey = context.getPrimaryKey();
        log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
                primaryKey);

        ProxyInterceptor<Object> interceptor = proxifier.getInterceptor(proxifiedEntity);
        Object entity = context.getEntity();

        interceptor.getDirtyMap().clear();

        Object freshEntity = loader.load(context, context.getEntityClass());

        if (freshEntity == null) {
            throw new AchillesStaleObjectStateException("The entity '" + entity + "' with primary_key '" + primaryKey
                    + "' no longer exists in Cassandra");
        }
        interceptor.setTarget(freshEntity);
        interceptor.getAlreadyLoaded().clear();
        interceptor.getAlreadyLoaded().addAll(context.getAllGettersExceptCounters());
    }

    public static enum Singleton {
        INSTANCE;

        private final EntityRefresher instance = new EntityRefresher();

        public EntityRefresher get() {
            return instance;
        }
    }
}
