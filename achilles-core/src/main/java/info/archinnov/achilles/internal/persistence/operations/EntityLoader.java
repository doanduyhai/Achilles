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

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.counterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Row;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyType;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.CounterBuilder;

public class EntityLoader {

    private static final Logger log  = LoggerFactory.getLogger(EntityLoader.class);

    private EntityMapper mapper = new EntityMapper();
    private ConsistencyOverrider overrider = new ConsistencyOverrider();

	public <T> T load(PersistenceContext context, Class<T> entityClass) {
        log.debug("Loading entity of class {} using PersistenceContext {}",entityClass,context);
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
		Validator
				.validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

        T entity = null;

        if (entityMeta.isClusteredCounter()) {
            ConsistencyLevel readLevel = overrider.getReadLevel(context,entityMeta);
            Row row =context.getClusteredCounter(readLevel);
            if(row != null) {
                entity = entityMeta.instanciate();
                entityMeta.getIdMeta().setValueToField(entity, primaryKey);

                for(PropertyMeta counterMeta:context.getAllCountersMeta()) {
                    mapper.setCounterToEntity(counterMeta, entity, row);
                }
            }
        } else {
            Row row = context.loadEntity();
            if (row != null) {
                entity = entityMeta.instanciate();
                mapper.setNonCounterPropertiesToEntity(row, entityMeta, entity);
            }
        }
		entityMeta.getIdMeta().setValueToField(entity, primaryKey);

		return entity;
	}

    public <T> T createEmptyEntity(PersistenceContext context, Class<T> entityClass) {
        log.debug("Loading entity of class {} using PersistenceContext {}", entityClass, context);
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        Validator.validateNotNull(entityClass, "Entity class should not be null");
        Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
        Validator
                .validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

        T entity = entityMeta.instanciate();
        entityMeta.getIdMeta().setValueToField(entity, primaryKey);

        return entity;
    }

    public void loadPropertyIntoObject(PersistenceContext context, Object realObject, PropertyMeta pm) {
        log.trace("Loading property {} into object {}",pm.getPropertyName(),realObject);
        PropertyType type = pm.type();
        if (type.isCounter()) {
            loadCounter(context, realObject, pm);
        } else {
            loadPropertyIntoEntity(context, pm, realObject);
        }

    }

    public void loadClusteredCounterColumn(PersistenceContext context, Object realObject, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context,counterMeta);
        Long counterValue = context.getClusteredCounterColumn(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, realObject, counterValue);
    }

    private void loadCounter(PersistenceContext context, Object entity, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context,counterMeta);
        final Long initialCounterValue = context.getSimpleCounter(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, entity, initialCounterValue);
    }

    private void loadPropertyIntoEntity(PersistenceContext context, PropertyMeta pm, Object entity) {
        Row row = context.loadProperty(pm);
        mapper.setPropertyToEntity(row, pm, entity);
    }
}
