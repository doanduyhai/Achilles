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
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

public class CounterLoader {

    private EntityMapper mapper = new EntityMapper();
    private ConsistencyOverrider overrider = new ConsistencyOverrider();

    public <T> T loadClusteredCounters(PersistenceContext context) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        T entity = null;
        Row row = context.getClusteredCounter();
        if (row != null) {
            entity = entityMeta.instanciate();
            entityMeta.getIdMeta().setValueToField(entity, primaryKey);

            for (PropertyMeta counterMeta : context.getAllCountersMeta()) {
                mapper.setCounterToEntity(counterMeta, entity, row);
            }
        }
        return entity;
    }


    public void loadClusteredCounterColumn(PersistenceContext context, Object realObject, PropertyMeta counterMeta) {
        Long counterValue = context.getClusteredCounterColumn(counterMeta);
        mapper.setCounterToEntity(counterMeta, realObject, counterValue);
    }

    public void loadCounter(PersistenceContext context, Object entity, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context, counterMeta);
        final Long initialCounterValue = context.getSimpleCounter(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, entity, initialCounterValue);
    }

}
