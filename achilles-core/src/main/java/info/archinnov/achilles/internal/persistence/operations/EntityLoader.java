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
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

public class EntityLoader {

    private static final Logger log = LoggerFactory.getLogger(EntityLoader.class);

    private EntityMapper mapper = EntityMapper.Singleton.INSTANCE.get();
    private CounterLoader counterLoader = CounterLoader.Singleton.INSTANCE.get();
    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    public <T> AchillesFuture<T> load(EntityOperations context, Class<T> entityClass) {
        log.debug("Loading entity of class {} using PersistenceContext {}", entityClass, context);
        final EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        Validator.validateNotNull(entityClass, "Entity class should not be null");
        Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
        Validator.validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

        AchillesFuture<T> achillesFuture;

        if (entityMeta.structure().isClusteredCounter()) {
            achillesFuture = counterLoader.loadClusteredCounters(context);
        } else {
            final ListenableFuture<Row> futureRow = context.loadEntity();
            Function<Row, T> rowToEntity = new Function<Row, T>() {
                @Override
                public T apply(Row row) {
                    T entity = null;
                    if (row != null) {
                        entity = entityMeta.forOperations().instanciate();
                        mapper.setNonCounterPropertiesToEntity(row, entityMeta, entity);
                    }
                    return entity;
                }
            };

            //No need executor service because already provided inside context.loadEntity()
            final ListenableFuture<T> futureEntity = asyncUtils.transformFuture(futureRow, rowToEntity);
            achillesFuture = asyncUtils.buildInterruptible(futureEntity);
        }
        return achillesFuture;
    }

    public <T> T createEmptyEntity(EntityOperations context, Class<T> entityClass) {
        log.debug("Loading entity of class {} using PersistenceContext {}", entityClass, context);
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        Validator.validateNotNull(entityClass, "Entity class should not be null");
        Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
        Validator.validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

        T entity = entityMeta.forOperations().instanciate();
        entityMeta.getIdMeta().forValues().setValueToField(entity, primaryKey);

        return entity;
    }

    public void loadPropertyIntoObject(EntityOperations context, Object realObject, PropertyMeta pm) {
        log.trace("Loading property {} into object {}", pm.getPropertyName(), realObject);
        if (pm.structure().isCounter()) {
            counterLoader.loadCounter(context, realObject, pm);
        } else {
            Row row = context.loadProperty(pm);
            if (row == null && pm.structure().isCollectionAndMap()) {
                row = new NullRow();
            }
            mapper.setPropertyToEntity(row, context.getEntityMeta(), pm, realObject);
        }

    }

    public static enum Singleton {
        INSTANCE;

        private final EntityLoader instance = new EntityLoader();

        public EntityLoader get() {
            return instance;
        }
    }
}
