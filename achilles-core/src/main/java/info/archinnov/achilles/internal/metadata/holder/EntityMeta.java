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
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.counterType;
import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isAssignableFromNativeType;
import static info.archinnov.achilles.type.Options.CASCondition;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.schemabuilder.Create;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;

public class EntityMeta {

    public static final boolean IS_MANAGED = true;
    public static final boolean IS_NOT_MANAGED = false;

    public static final Predicate<EntityMeta> CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta) {
            return meta.isClusteredCounter();
        }
    };

    public static final Predicate<EntityMeta> EXCLUDE_CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta) {
            return !meta.isClusteredCounter();
        }
    };


    private ReflectionInvoker invoker = new ReflectionInvoker();

    private Class<?> entityClass;
    private String className;
    private String tableName;
    private String tableComment;
    private Class<?> idClass;
    private Map<String, PropertyMeta> propertyMetas;
    private List<PropertyMeta> allMetasExceptCounters;
    private List<PropertyMeta> allMetasExceptIdAndCounters;
    private PropertyMeta idMeta;
    private Map<Method, PropertyMeta> getterMetas;
    private Map<Method, PropertyMeta> setterMetas;
    private boolean clusteredEntity = false;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private List<PropertyMeta> allMetasExceptId;
    private boolean clusteredCounter = false;
    private List<Interceptor<?>> interceptors = new ArrayList<>();
    private InsertStrategy insertStrategy;
    private boolean schemaUpdateEnabled = false;
    private boolean hasOnlyStaticColumns = false;

    public Object getPrimaryKey(Object entity) {
        return idMeta.getPrimaryKey(entity);
    }

    public void addInterceptor(Interceptor<?> interceptor) {
        interceptors.add(interceptor);
    }

    public List<Interceptor<?>> getInterceptors() {
        return interceptors;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void intercept(Object entity, Event event) {
        List<Interceptor<?>> interceptors = getInterceptorsForEvent(event);
        if (interceptors.size() > 0) {
            for (Interceptor interceptor : interceptors) {
                interceptor.onEvent(entity);
            }
            Validator.validateNotNull(getPrimaryKey(entity),
                    "The primary key should not be null after intercepting the event '%s'", event);
        }
    }

    protected List<Interceptor<?>> getInterceptorsForEvent(final Event event) {
        return FluentIterable.from(interceptors).filter(getFilterForEvent(event)).toList();

    }

    private Predicate<? super Interceptor<?>> getFilterForEvent(final Event event) {
        return new Predicate<Interceptor<?>>() {
            public boolean apply(Interceptor<?> p) {
                return p != null && p.events() != null && p.events().contains(event);
            }
        };
    }


    public void validatePartitionComponents(Object...partitionComponents) {
        idMeta.validatePartitionComponents(partitionComponents);
    }

    public void validatePartitionComponentsIn(Object...partitionComponents) {
        idMeta.validatePartitionComponentsIn(partitionComponents);
    }

    public void validateClusteringComponents(Object...clusteringComponents) {
        idMeta.validateClusteringComponents(clusteringComponents);
    }

    public void validateClusteringComponentsIn(Object...clusteringComponents) {
        idMeta.validateClusteringComponentsIn(clusteringComponents);
    }

    public List<String> getPartitionKeysName(int size) {
        return idMeta.getPartitionComponentNames().subList(0,size);
    }

    public String getLastPartitionKeyName() {
        final List<String> partitionComponentNames = idMeta.getPartitionComponentNames();
        return partitionComponentNames.get(partitionComponentNames.size()-1);
    }

    public List<String> getClusteringKeysName(int size) {
        return idMeta.getClusteringComponentNames().subList(0,size);
    }

    public String getLastClusteringKeyName() {
        final List<String> clusteringComponentNames = idMeta.getClusteringComponentNames();
        return clusteringComponentNames.get(clusteringComponentNames.size()-1);
    }

    public int getPartitionKeysSize() {
        return idMeta.getPartitionComponentClasses().size();
    }

    public int getClusteringKeysSize() {
        return idMeta.getClusteringComponentClasses().size();
    }

    public List<Create.Options.ClusteringOrder> getClusteringOrders() {
        return idMeta.getClusteringOrders();
    }

    @SuppressWarnings("unchecked")
    public <T> T instanciate() {
        return (T) invoker.instantiate(entityClass);
    }

    public boolean hasEmbeddedId() {
        return idMeta.isEmbeddedId();
    }

    // ////////// Getters & Setters
    @SuppressWarnings("unchecked")
    public <T> Class<T> getEntityClass() {
        return (Class<T>) entityClass;
    }

    public void setEntityClass(Class<?> entityClass) {
        this.entityClass = entityClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public Map<String, PropertyMeta> getPropertyMetas() {
        return propertyMetas;
    }

    public void setPropertyMetas(Map<String, PropertyMeta> propertyMetas) {
        this.propertyMetas = propertyMetas;
    }

    public PropertyMeta getIdMeta() {
        return idMeta;
    }

    public void setIdMeta(PropertyMeta idMeta) {
        this.idMeta = idMeta;
    }

    public Map<Method, PropertyMeta> getGetterMetas() {
        return getterMetas;
    }

    public void setGetterMetas(Map<Method, PropertyMeta> getterMetas) {
        this.getterMetas = getterMetas;
    }

    public Map<Method, PropertyMeta> getSetterMetas() {
        return setterMetas;
    }

    public void setSetterMetas(Map<Method, PropertyMeta> setterMetas) {
        this.setterMetas = setterMetas;
    }

    public boolean isClusteredEntity() {
        return clusteredEntity;
    }

    public void setClusteredEntity(boolean clusteredEntity) {
        this.clusteredEntity = clusteredEntity;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return this.consistencyLevels.left;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return this.consistencyLevels.right;
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels() {
        return this.consistencyLevels;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    public InsertStrategy getInsertStrategy() {
        return insertStrategy;
    }

    public void setInsertStrategy(InsertStrategy insertStrategy) {
        this.insertStrategy = insertStrategy;
    }

    public boolean isSchemaUpdateEnabled() {
        return schemaUpdateEnabled;
    }

    public void setSchemaUpdateEnabled(boolean schemaUpdateEnabled) {
        this.schemaUpdateEnabled = schemaUpdateEnabled;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getIdClass() {
        return (Class<T>) idClass;
    }

    public void setIdClass(Class<?> idClass) {
        this.idClass = idClass;
    }

    public List<PropertyMeta> getAllMetas() {
        return new ArrayList<>(propertyMetas.values());
    }

    public List<PropertyMeta> getAllCounterMetas() {
        return from(propertyMetas.values()).filter(counterType).toList();
    }

    public boolean isClusteredCounter() {
        return this.clusteredCounter;
    }

    public void setClusteredCounter(boolean clusteredCounter) {
        this.clusteredCounter = clusteredCounter;
    }

    public boolean isValueless() {
        return propertyMetas.size() == 1;
    }

    public List<PropertyMeta> getAllMetasExceptId() {
        return allMetasExceptId;
    }

    public void setAllMetasExceptId(List<PropertyMeta> allMetasExceptId) {
        this.allMetasExceptId = allMetasExceptId;
    }

    public List<PropertyMeta> getAllMetasExceptIdAndCounters() {
        return allMetasExceptIdAndCounters;
    }

    public void setAllMetasExceptIdAndCounters(List<PropertyMeta> allMetasExceptIdAndCounters) {
        this.allMetasExceptIdAndCounters = allMetasExceptIdAndCounters;
    }

    public List<PropertyMeta> getAllMetasExceptCounters() {
        return allMetasExceptCounters;
    }

    public void setAllMetasExceptCounters(List<PropertyMeta> allMetasExceptCounters) {
        this.allMetasExceptCounters = allMetasExceptCounters;
    }

    public boolean hasOnlyStaticColumns() {
        return hasOnlyStaticColumns;
    }

    public void setHasOnlyStaticColumns(boolean hasOnlyStaticColumns) {
        this.hasOnlyStaticColumns = hasOnlyStaticColumns;
    }

    public List<PropertyMeta> getColumnsMetaToInsert() {
        if (clusteredCounter) {
            return allMetasExceptId;
        } else {
            return allMetasExceptIdAndCounters;
        }
    }

    public List<PropertyMeta> getColumnsMetaToLoad() {
        if (clusteredCounter) {
            return new ArrayList<>(propertyMetas.values());
        } else {
            return allMetasExceptCounters;
        }
    }

    public Object[] encodeBoundValuesForTypedQueries(Object[] boundValues) {
        Object[] encodedBoundValues = new Object[boundValues != null ? boundValues.length : 0];
        for (int i = 0; i < encodedBoundValues.length; i++) {
            final Object boundValue = boundValues[i];
            if (boundValue != null) {
                final Class<?> type = boundValue.getClass();
                if (isAssignableFromNativeType(type)) {
                    encodedBoundValues[i] = boundValue;
                } else if (type.isEnum()) {
                    encodedBoundValues[i] = ((Enum<?>) boundValue).name();
                } else {
                    throw new AchillesException("Cannot encode value " + boundValue + " for typed query");
                }
            }
        }
        return encodedBoundValues;
    }

    public Object encodeCasConditionValue(CASCondition CASCondition) {
        Object rawValue = CASCondition.getValue();
        final String columnName = CASCondition.getColumnName();
        Object encodedValue = encodeValueForProperty(columnName, rawValue);
        CASCondition.encodeValue(encodedValue);
        return encodedValue;
    }

    public Object encodeIndexConditionValue(IndexCondition indexCondition) {
        Object rawValue = indexCondition.getColumnValue();
        final String columnName = indexCondition.getColumnName();
        Object encodedValue = encodeValueForProperty(columnName, rawValue);
        indexCondition.encodeValue(encodedValue);
        return encodedValue;
    }

    private Object encodeValueForProperty(String columnName, Object rawValue) {
        Object encodedValue = rawValue;
        if (rawValue != null) {
            final PropertyMeta propertyMeta = findPropertyMetaByCQL3Name(columnName);
            encodedValue = propertyMeta.encode(rawValue);
        }
        return encodedValue;
    }

    public List<PropertyMeta> retrievePropertyMetasForInsert(Object entity) {
        if (insertStrategy == InsertStrategy.ALL_FIELDS) {
            return this.getAllMetasExceptIdAndCounters();
        }

        List<PropertyMeta> metasForNonNullProperties = new ArrayList<>();
        for (PropertyMeta propertyMeta : this.getAllMetasExceptIdAndCounters()) {
            if (propertyMeta.getValueFromField(entity) != null) {
                metasForNonNullProperties.add(propertyMeta);
            }
        }
        return metasForNonNullProperties;
    }

    private PropertyMeta findPropertyMetaByCQL3Name(String cql3Name) {
        for (PropertyMeta meta : allMetasExceptCounters) {
            if (meta.getCQL3PropertyName().equals(cql3Name)) {
                return meta;
            }
        }
        throw new AchillesException(String.format("Cannot find matching property meta for the cql3 field %s", cql3Name));
    }

    @Override
    public String toString() {

        final ArrayList<String> propertyNames = new ArrayList<>(propertyMetas.keySet());
        Collections.sort(propertyNames);
        return Objects.toStringHelper(this.getClass()).add("className", className)
                .add("tableName/tableName", tableName)
                .add("propertyMetas", StringUtils.join(propertyNames, ",")).add("idMeta", idMeta)
                .add("clusteredEntity", clusteredEntity).add("consistencyLevels", consistencyLevels).toString();
    }

    public static enum EntityState {
        MANAGED(true),
        NOT_MANAGED(false);

        private final boolean managed;

        EntityState(boolean managed) {
            this.managed = managed;
        }

        public boolean isManaged() {
            return managed;
        }
    }
}
