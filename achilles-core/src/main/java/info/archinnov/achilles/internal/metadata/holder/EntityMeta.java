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
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER_TYPE;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;

public class EntityMeta {

    public static final Predicate<EntityMeta> CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta) {
            return meta.clusteredCounter;
        }
    };

    public static final Predicate<EntityMeta> EXCLUDE_CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta) {
            return !meta.clusteredCounter;
        }
    };


    private Class<?> entityClass;
    private String className;
    protected String tableName;
    protected String keyspaceName;
    protected String qualifiedTableName;
    protected String tableComment;
    private Class<?> idClass;
    private Map<String, PropertyMeta> propertyMetas;
    private List<PropertyMeta> allMetasExceptCounters;
    private List<PropertyMeta> allMetasExceptIdAndCounters;
    protected PropertyMeta idMeta;
    private Map<Method, PropertyMeta> getterMetas;
    private Map<Method, PropertyMeta> setterMetas;
    private List<PropertyMeta> allMetasExceptId;

    protected boolean clusteredEntity = false;
    protected boolean clusteredCounter = false;
    protected boolean hasOnlyStaticColumns = false;
    protected boolean hasStaticColumns = false;

    protected List<Interceptor<?>> interceptors = new ArrayList<>();

    protected Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    protected InsertStrategy insertStrategy;
    protected boolean schemaUpdateEnabled = false;

    private final EntityMetaInterceptors forInterception;
    private final EntityMetaSliceQuerySupport forSliceQuery;
    private final EntityMetaTranscoder forTranscoding;
    private final EntityMetaOperations forOperations;
    private final EntityMetaStructure structure;
    private final EntityMetaConfig config;

    public EntityMeta() {
        // We can safely create singleton instance of entity meta views here since those views
        // only require a reference on the EntityMeta class. Any mutation on the EntityMeta
        // instance are transparent to the views
        this.forInterception = new EntityMetaInterceptors(this);
        this.forSliceQuery = new EntityMetaSliceQuerySupport(this);
        this.forTranscoding = new EntityMetaTranscoder(this);
        this.forOperations = new EntityMetaOperations(this);
        this.structure = new EntityMetaStructure(this);
        this.config = new EntityMetaConfig(this);
    }

    public EntityMetaInterceptors forInterception() {
        return forInterception;
    }

    public EntityMetaSliceQuerySupport forSliceQuery() {
        return forSliceQuery;
    }

    public EntityMetaTranscoder forTranscoding() {
        return forTranscoding;
    }

    public EntityMetaOperations forOperations() {
        return forOperations;
    }

    public EntityMetaStructure structure() {
        return structure;
    }

    public EntityMetaConfig config() {
        return config;
    }


    // ////////// Getters & Setters
    @SuppressWarnings("unchecked")
    public <T> Class<T> getEntityClass() {
        return (Class<T>) entityClass;
    }

    void setEntityClass(Class<?> entityClass) {
        this.entityClass = entityClass;
    }

    public String getClassName() {
        return className;
    }

    void setClassName(String className) {
        this.className = className;
    }

    void setQualifiedTableName(String qualifiedTableName) {
        this.qualifiedTableName = qualifiedTableName;
    }

    void setTableName(String tableName) {
        this.tableName = tableName;
    }

    void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public Map<String, PropertyMeta> getPropertyMetas() {
        return propertyMetas;
    }

    void setPropertyMetas(Map<String, PropertyMeta> propertyMetas) {
        this.propertyMetas = propertyMetas;
    }

    public PropertyMeta getIdMeta() {
        return idMeta;
    }

    void setIdMeta(PropertyMeta idMeta) {
        this.idMeta = idMeta;
    }

    public Map<Method, PropertyMeta> getGetterMetas() {
        return getterMetas;
    }

    void setGetterMetas(Map<Method, PropertyMeta> getterMetas) {
        this.getterMetas = getterMetas;
    }

    public Map<Method, PropertyMeta> getSetterMetas() {
        return setterMetas;
    }

    void setSetterMetas(Map<Method, PropertyMeta> setterMetas) {
        this.setterMetas = setterMetas;
    }

    void setClusteredEntity(boolean clusteredEntity) {
        this.clusteredEntity = clusteredEntity;
    }

    void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    void setInsertStrategy(InsertStrategy insertStrategy) {
        this.insertStrategy = insertStrategy;
    }

    void setSchemaUpdateEnabled(boolean schemaUpdateEnabled) {
        this.schemaUpdateEnabled = schemaUpdateEnabled;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getIdClass() {
        return (Class<T>) idClass;
    }

    void setIdClass(Class<?> idClass) {
        this.idClass = idClass;
    }

    public List<PropertyMeta> getAllMetas() {
        return new ArrayList<>(propertyMetas.values());
    }

    public List<PropertyMeta> getAllCounterMetas() {
        return from(propertyMetas.values()).filter(COUNTER_TYPE).toList();
    }

    void setClusteredCounter(boolean clusteredCounter) {
        this.clusteredCounter = clusteredCounter;
    }

    public List<PropertyMeta> getAllMetasExceptId() {
        return allMetasExceptId;
    }

    void setAllMetasExceptId(List<PropertyMeta> allMetasExceptId) {
        this.allMetasExceptId = allMetasExceptId;
    }

    public List<PropertyMeta> getAllMetasExceptIdAndCounters() {
        return allMetasExceptIdAndCounters;
    }

    void setAllMetasExceptIdAndCounters(List<PropertyMeta> allMetasExceptIdAndCounters) {
        this.allMetasExceptIdAndCounters = allMetasExceptIdAndCounters;
    }

    public List<PropertyMeta> getAllMetasExceptCounters() {
        return allMetasExceptCounters;
    }

    void setAllMetasExceptCounters(List<PropertyMeta> allMetasExceptCounters) {
        this.allMetasExceptCounters = allMetasExceptCounters;
    }

    void setHasOnlyStaticColumns(boolean hasOnlyStaticColumns) {
        this.hasOnlyStaticColumns = hasOnlyStaticColumns;
    }

    public boolean hasStaticColumns() {
        return hasStaticColumns;
    }

    public void setHasStaticColumns(boolean hasStaticColumns) {
        this.hasStaticColumns = hasStaticColumns;
    }

    @Override
    public String toString() {

        final ArrayList<String> propertyNames = new ArrayList<>(propertyMetas.keySet());
        Collections.sort(propertyNames);
        return Objects.toStringHelper(this.getClass()).add("className", className)
                .add("qualifiedTableName", qualifiedTableName)
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
