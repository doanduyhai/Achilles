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
package info.archinnov.achilles.internal.metadata.holder;

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.excludeCounterType;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.excludeIdAndCounterType;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.excludeIdType;
import static info.archinnov.achilles.internal.table.TableCreator.TABLE_PATTERN;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;

public class EntityMetaBuilder {
    private static final Logger log = LoggerFactory.getLogger(EntityMetaBuilder.class);

    private PropertyMeta idMeta;
    private Class<?> entityClass;
    private String className;
    private String columnFamilyName;
    private Map<String, PropertyMeta> propertyMetas;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private InsertStrategy insertStrategy;
    private boolean schemaUpdateEnabled;

    public static EntityMetaBuilder entityMetaBuilder(PropertyMeta idMeta) {
        return new EntityMetaBuilder(idMeta);
    }

    public EntityMetaBuilder(PropertyMeta idMeta) {
        this.idMeta = idMeta;
    }

    public EntityMeta build() {
        log.debug("Build entityMeta for entity class {}", className);

        Validator.validateNotNull(idMeta, "idMeta should not be null for entity meta creation");
        Validator.validateNotEmpty(propertyMetas, "propertyMetas map should not be empty for entity meta creation");
        Validator.validateRegExp(columnFamilyName, TABLE_PATTERN, "columnFamilyName for entity meta creation");

        EntityMeta meta = new EntityMeta();

        meta.setIdMeta(idMeta);
        meta.setIdClass(idMeta.getValueClass());
        meta.setEntityClass(entityClass);
        meta.setClassName(className);
        meta.setTableName(columnFamilyName);
        meta.setPropertyMetas(Collections.unmodifiableMap(propertyMetas));
        meta.setGetterMetas(Collections.unmodifiableMap(extractGetterMetas(propertyMetas)));
        meta.setSetterMetas(Collections.unmodifiableMap(extractSetterMetas(propertyMetas)));
        meta.setConsistencyLevels(consistencyLevels);
        meta.setInsertStrategy(insertStrategy);
        meta.setSchemaUpdateEnabled(schemaUpdateEnabled);

        List<PropertyMeta> allMetasExceptId = new ArrayList<>(from(propertyMetas.values()).filter(excludeIdType)
                .toList());
        meta.setAllMetasExceptId(allMetasExceptId);

        List<PropertyMeta> allMetasExceptIdAndCounters = new ArrayList<>(from(propertyMetas.values()).filter(
                excludeIdAndCounterType).toList());
        meta.setAllMetasExceptIdAndCounters(allMetasExceptIdAndCounters);

        List<PropertyMeta> allMetasExceptCounters = new ArrayList<>(from(propertyMetas.values()).filter(
                excludeCounterType).toList());
        meta.setAllMetasExceptCounters(allMetasExceptCounters);

        boolean clusteredEntity = idMeta.isEmbeddedId() && idMeta.getClusteringComponentClasses().size() > 0;
        meta.setClusteredEntity(clusteredEntity);

        boolean clusteredCounter = allMetasExceptId.size() > 0;
        for (PropertyMeta pm : allMetasExceptId) {
            if (!pm.isCounter()) {
                clusteredCounter = false;
                break;
            }
        }
        meta.setClusteredCounter(clusteredCounter);
        return meta;
    }

    private Map<Method, PropertyMeta> extractGetterMetas(Map<String, PropertyMeta> propertyMetas) {
        Map<Method, PropertyMeta> getterMetas = new HashMap<>();
        for (PropertyMeta propertyMeta : propertyMetas.values()) {
            getterMetas.put(propertyMeta.getGetter(), propertyMeta);
        }
        return getterMetas;
    }

    private Map<Method, PropertyMeta> extractSetterMetas(Map<String, PropertyMeta> propertyMetas) {
        Map<Method, PropertyMeta> setterMetas = new HashMap<>();
        for (PropertyMeta propertyMeta : propertyMetas.values()) {
            setterMetas.put(propertyMeta.getSetter(), propertyMeta);
        }
        return setterMetas;
    }

    public EntityMetaBuilder entityClass(Class<?> entityClass) {
        this.entityClass = entityClass;
        return this;
    }

    public EntityMetaBuilder className(String className) {
        this.className = className;
        return this;
    }

    public EntityMetaBuilder columnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
        return this;
    }

    public EntityMetaBuilder propertyMetas(Map<String, PropertyMeta> propertyMetas) {
        this.propertyMetas = propertyMetas;
        return this;
    }

    public EntityMetaBuilder consistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
        return this;
    }

    public EntityMetaBuilder insertStrategy(InsertStrategy insertStrategy) {
        this.insertStrategy = insertStrategy;
        return this;
    }

    public EntityMetaBuilder schemaUpdateEnabled(boolean value) {
        this.schemaUpdateEnabled = value;
        return this;
    }
}
