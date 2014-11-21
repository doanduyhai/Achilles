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

import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

public class CounterLoader {

    private EntityMapper mapper = EntityMapper.Singleton.INSTANCE.get();
    private ConsistencyOverrider overrider = ConsistencyOverrider.Singleton.INSTANCE.get();
    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    public <T> AchillesFuture<T> loadClusteredCounters(final EntityOperations context) {
        final EntityMeta entityMeta = context.getEntityMeta();
        final Object primaryKey = context.getPrimaryKey();

        final ListenableFuture<Row> futureRow = context.getClusteredCounter();
        Function<Row, T> rowToEntity = new Function<Row, T>() {
            @Override
            public T apply(Row row) {
                T entity = null;
                if (row != null) {
                    entity = entityMeta.forOperations().instanciate();
                    entityMeta.getIdMeta().forValues().setValueToField(entity, primaryKey);

                    for (PropertyMeta counterMeta : context.getAllCountersMeta()) {
                        mapper.setCounterToEntity(counterMeta, entity, row);
                    }
                }
                return entity;
            }
        };
        final ListenableFuture<T> futureEntity = asyncUtils.transformFuture(futureRow, rowToEntity);
        return asyncUtils.buildInterruptible(futureEntity);
    }


    public void loadClusteredCounterColumn(EntityOperations context, Object realObject, PropertyMeta counterMeta) {
        final Long counterValue = context.getClusteredCounterColumn(counterMeta);
        mapper.setCounterToEntity(counterMeta, realObject, counterValue);
    }

    public void loadCounter(EntityOperations context, Object entity, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context, counterMeta);
        final Long initialCounterValue = context.getSimpleCounter(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, entity, initialCounterValue);
    }

    public static enum Singleton {
        INSTANCE;

        private final CounterLoader instance = new CounterLoader();

        public CounterLoader get() {
            return instance;
        }
    }
}
