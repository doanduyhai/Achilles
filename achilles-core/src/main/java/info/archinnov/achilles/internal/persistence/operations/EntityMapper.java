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

import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import java.util.Map;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaRowExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.type.Counter;

public class EntityMapper {

    private static final Logger log = LoggerFactory.getLogger(EntityMapper.class);

    private RowMethodInvoker cqlRowInvoker = RowMethodInvoker.Singleton.INSTANCE.get();

    public void setNonCounterPropertiesToEntity(Row row, EntityMeta entityMeta, Object entity, EntityState entityState) {
        log.debug("Set non-counter properties to entity class {} from fetched CQL row", entityMeta.getClassName());
        for (PropertyMeta pm : entityMeta.getAllMetasExceptCounters()) {
            setPropertyToEntity(row, entityMeta, pm, entity, entityState);
        }
    }

    public void setPropertyToEntity(Row row, EntityMeta meta,PropertyMeta pm, Object entity, EntityState entityState) {
        log.debug("Set property {} value from fetched CQL row", pm.getPropertyName());
        if (row != null) {
            final PropertyMetaRowExtractor rowExtractor = pm.forRowExtraction();
            if (pm.structure().isCompoundPK()) {
                Object compoundKey = rowExtractor.extractCompoundPrimaryKeyFromRow(row, meta, entityState);
                pm.forValues().setValueToField(entity, compoundKey);
            } else {
                Object value = rowExtractor.invokeOnRowForFields(row);
                pm.forValues().setValueToField(entity, value);
            }
        }
    }

    public <T> T mapRowToEntityWithPrimaryKey(EntityMeta meta, Row row, Map<String, PropertyMeta> propertiesMap, EntityState entityState) {
        log.debug("Map CQL row to entity of class {}", meta.getClassName());
        T entity = null;
        ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        if (columnDefinitions != null) {
            entity = meta.forOperations().instanciate();
            for (Definition column : columnDefinitions) {
                String columnName = column.getName();
                PropertyMeta pm = propertiesMap.get(columnName);
                if (pm != null) {
                    Object value = pm.forRowExtraction().invokeOnRowForFields(row);
                    pm.forValues().setValueToField(entity, value);
                }
            }
            PropertyMeta idMeta = meta.getIdMeta();
            if (idMeta.structure().isCompoundPK()) {
                Object compoundKey = idMeta.forRowExtraction().extractCompoundPrimaryKeyFromRow(row, meta, entityState);
                idMeta.forValues().setValueToField(entity, compoundKey);
            }
        }
        return entity;
    }

    public void setValuesToClusteredCounterEntity(Row row, EntityMeta entityMeta, Object clusteredEntity) {
        log.debug("Set values to clustered counter entity class {} from fetched CQL row", entityMeta.getClassName());
        for (PropertyMeta pm : entityMeta.getAllCounterMetas()) {
            setCounterToEntity(pm, clusteredEntity, row);
        }
    }

    public void setCounterToEntity(PropertyMeta counterMeta, Object entity, Long counterValue) {
        if (log.isDebugEnabled()) {
            log.debug("Set counter value {} to property {} of entity class {}", counterValue, counterMeta.getPropertyName(), counterMeta.getEntityClassName());
        }
        final Counter counter = InternalCounterBuilder.initialValue(counterValue);
        counterMeta.forValues().setValueToField(entity, counter);
    }

    public void setCounterToEntity(PropertyMeta counterMeta, Object entity, Row row) {
        log.debug("Set counter value to property {} of entity class {} from CQL row", counterMeta.getPropertyName(), counterMeta.getEntityClassName());
        Long counterValue = cqlRowInvoker.invokeOnRowForType(row, Long.class, counterMeta.getCQLColumnName());
        setCounterToEntity(counterMeta, entity, counterValue);
    }

    public static enum Singleton {
        INSTANCE;

        private final EntityMapper instance = new EntityMapper();

        public EntityMapper get() {
            return instance;
        }
    }
}
