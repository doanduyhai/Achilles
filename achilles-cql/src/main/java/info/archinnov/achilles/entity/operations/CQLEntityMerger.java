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

import java.lang.reflect.Method;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLMergerImpl;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.validation.Validator;

public class CQLEntityMerger {

    private static final Logger log = LoggerFactory.getLogger(CQLEntityMerger.class);

    private CQLMergerImpl merger = new CQLMergerImpl();
    private CQLEntityPersister persister = new CQLEntityPersister();
    private CQLEntityProxifier proxifier = new CQLEntityProxifier();

    public <T> T merge(CQLPersistenceContext context, T entity) {
        log.debug("Merging entity of class {} with primary key {}", context.getEntityClass().getCanonicalName(),
                  context.getPrimaryKey());

        EntityMeta entityMeta = context.getEntityMeta();

        Validator.validateNotNull(entity, "Proxy object should not be null for merge");
        Validator.validateNotNull(entityMeta, "entityMeta should not be null for merge");

        T proxy;
        if (proxifier.isProxy(entity)) {
            log.debug("Checking for dirty fields before merging");

            T realObject = proxifier.getRealObject(entity);
            context.setEntity(realObject);

            CQLEntityInterceptor<T> interceptor = proxifier.getInterceptor(entity);
            Map<Method, PropertyMeta> dirtyMap = interceptor.getDirtyMap();
            merger.merge(context, dirtyMap);
            interceptor.setContext(context);
            interceptor.setTarget(realObject);
            proxy = entity;
        } else {
            log.debug("Persisting transient entity");

            persister.persist(context);
            proxy = proxifier.buildProxy(entity, context);
        }
        return proxy;
    }
}
