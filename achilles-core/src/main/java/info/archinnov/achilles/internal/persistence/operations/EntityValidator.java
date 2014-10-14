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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

public class EntityValidator {
    private static final Logger log = LoggerFactory.getLogger(EntityValidator.class);

    private EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();

    public void validateEntity(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
        Validator.validateNotNull(entity, "Entity should not be null");

        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);
        validateEntity(entity, entityMeta);

    }

    public void validateEntity(Object entity, EntityMeta entityMeta) {
        log.trace("Validate entity {}", entity);
        Validator.validateNotNull(entityMeta, "The entity %s is not managed by Achilles", entity.getClass().getCanonicalName());

        Object rawEntity = proxifier.getRealObject(entity);
        Object id = entityMeta.forOperations().getPrimaryKey(rawEntity);
        if (id == null) {
            throw new IllegalArgumentException("Cannot get primary key for entity " + rawEntity.getClass().getCanonicalName());
        }
        validatePrimaryKey(entityMeta.getIdMeta(), id);
    }

    public void validatePrimaryKey(PropertyMeta idMeta, Object primaryKey) {
        log.trace("Validate primary key {} for entity class {}", primaryKey, idMeta.getEntityClassName());
        if (idMeta.structure().isEmbeddedId()) {
            List<Object> components = idMeta.forTranscoding().encodeToComponents(primaryKey, false);
            for (Object component : components) {
                Validator.validateNotNull(component, "The compound primary key '%s' components should not be null", idMeta.getPropertyName());
            }
        }
    }

    public void validateNotClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
        log.trace("Validate that entity {} is not a clustered counter", entity);
        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);
        Validator.validateFalse(entityMeta.structure().isClusteredCounter(),
                "The entity '%s' is a clustered counter and does not support insert/update with TTL", entity);
    }

    public static enum Singleton {
        INSTANCE;

        private final EntityValidator instance = new EntityValidator();

        public EntityValidator get() {
            return instance;
        }
    }
}
