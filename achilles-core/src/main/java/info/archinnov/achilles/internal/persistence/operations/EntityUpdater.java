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
package info.archinnov.achilles.internal.persistence.operations;

import java.lang.reflect.Method;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.impl.UpdaterImpl;
import info.archinnov.achilles.internal.proxy.EntityInterceptor;
import info.archinnov.achilles.internal.validation.Validator;

public class EntityUpdater {

    private static final Logger log = LoggerFactory.getLogger(EntityUpdater.class);

    private UpdaterImpl updater = new UpdaterImpl();
    private EntityPersister persister = new EntityPersister();
    private EntityProxifier proxifier = new EntityProxifier();

    public void update(PersistenceContext context, Object entity) {
        log.debug("Merging entity of class {} with primary key {}", context.getEntityClass().getCanonicalName(),
                  context.getPrimaryKey());

        EntityMeta entityMeta = context.getEntityMeta();

        Validator.validateNotNull(entity, "Proxy object should not be null for update");
        Validator.validateNotNull(entityMeta, "entityMeta should not be null for update");

        log.debug("Checking for dirty fields before merging");

        Object realObject = proxifier.getRealObject(entity);
        context.setEntity(realObject);

        EntityInterceptor<Object> interceptor = proxifier.getInterceptor(entity);
        Map<Method, PropertyMeta> dirtyMap = interceptor.getDirtyMap();
        updater.update(context, dirtyMap);
        interceptor.setContext(context);
        interceptor.setTarget(realObject);
    }
}
