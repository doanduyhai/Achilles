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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.json.DefaultJacksonMapper;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyMeta {


    public static final Predicate<PropertyMeta> STATIC_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.structure().isStaticColumn();
        }
    };

    public static final Predicate<PropertyMeta> COUNTER_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.structure().isCounter();
        }
    };

    public static final Function<PropertyMeta,String> GET_CQL_COLUMN_NAME = new Function<PropertyMeta, String>() {
        @Override
        public String apply(PropertyMeta meta) {
            return meta.getCQLColumnName();
        }
    };

    public static final Function<String,String> TO_LOWER_CASE = new Function<String, String>() {
        @Override
        public String apply(String name) {
            return name.toLowerCase();
        }
    };

    public static final Function<PropertyMeta, List<String>> GET_CQL_COLUMN_NAMES_FROM_COMPOUND_PK = new Function<PropertyMeta, List<String>>() {
        @Override
        public List<String> apply(PropertyMeta compoundPKMeta) {
            return compoundPKMeta.getCompoundPKProperties().getCQLComponentNames();
        }
    };

    ObjectMapper defaultJacksonMapperForCounter = DefaultJacksonMapper.COUNTER.get();

    private CompoundPKProperties compoundPKProperties;
    private String entityClassName;
    private Class<?> valueClass;
    private Class<?> keyClass;
    private Class<?> cqlValueClass;
    private Class<?> cqlKeyClass;
    private PropertyType type;
    String propertyName;
    String cqlColumnName;
    Method getter;
    Method setter;
    private Field field;
    CounterProperties counterProperties;
    private IndexProperties indexProperties;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private boolean timeUUID = false;
    private boolean emptyCollectionAndMapIfNull = false;
    private boolean staticColumn = false;
    private Codec simpleCodec;
    private ListCodec listCodec;
    private SetCodec setCodec;
    private MapCodec mapCodec;

    private final PropertyMetaRowExtractor forRowExtraction;
    private final PropertyMetaStatementGenerator forStatementGeneration;
    private final PropertyMetaCacheSupport forCache;
    private final PropertyMetaTableValidator forTableValidation;
    private final PropertyMetaTableCreator forTableCreation;
    private final PropertyMetaSliceQuerySupport forSliceQuery;
    private final PropertyMetaSliceQueryContext forSliceQueryContext;
    private final PropertyMetaTypedQuery forTypedQuery;
    private final PropertyMetaTranscoder forTranscoding;
    private final PropertyMetaStructure structure;
    private final PropertyMetaConfig config;
    private final PropertyMetaValues forValues;

    public PropertyMeta() {
        this.forRowExtraction = new PropertyMetaRowExtractor(this);
        this.forStatementGeneration = new PropertyMetaStatementGenerator(this);
        this.forCache = new PropertyMetaCacheSupport(this);
        this.forTableValidation = new PropertyMetaTableValidator(this);
        this.forTableCreation = new PropertyMetaTableCreator(this);
        this.forSliceQuery = new PropertyMetaSliceQuerySupport(this);
        this.forSliceQueryContext = new PropertyMetaSliceQueryContext(this);
        this.forTypedQuery = new PropertyMetaTypedQuery(this);
        this.forTranscoding = new PropertyMetaTranscoder(this);
        this.structure = new PropertyMetaStructure(this);
        this.config = new PropertyMetaConfig(this);
        this.forValues = new PropertyMetaValues(this);
    }

    public PropertyMetaRowExtractor forRowExtraction() {
        return forRowExtraction;
    }

    public PropertyMetaStatementGenerator forStatementGeneration() {
        return forStatementGeneration;
    }

    public PropertyMetaCacheSupport forCache() {
        return forCache;
    }

    public PropertyMetaTableValidator forTableValidation() {
        return forTableValidation;
    }

    public PropertyMetaTableCreator forTableCreation() {
        return forTableCreation;
    }

    public PropertyMetaSliceQuerySupport forSliceQuery() {
        return forSliceQuery;
    }

    public PropertyMetaSliceQueryContext forSliceQueryContext() {
        return forSliceQueryContext;
    }

    public PropertyMetaTypedQuery forTypedQuery() {
        return forTypedQuery;
    }

    public PropertyMetaTranscoder forTranscoding() {
        return forTranscoding;
    }

    public PropertyMetaStructure structure() {
        return structure;
    }

    public PropertyMetaConfig config() {
        return config;
    }

    public PropertyMetaValues forValues() {
        return forValues;
    }


    // //////// Getters & setters
    public PropertyType type() {
        return type;
    }

    public void setType(PropertyType propertyType) {
        this.type = propertyType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getKeyClass() {
        return (Class<T>) keyClass;
    }

    public void setKeyClass(Class<?> keyClass) {
        this.keyClass = keyClass;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getValueClass() {
        return (Class<T>) valueClass;
    }

    public void setValueClass(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    <T> Class<T> getCqlValueClass() {
        return (Class<T>) cqlValueClass;
    }

    void setCqlValueClass(Class<?> cqlValueClass) {
        this.cqlValueClass = cqlValueClass;
    }

    <T> Class<T> getCQLKeyClass() {
        return (Class<T>) cqlKeyClass;
    }

    void setCQLKeyClass(Class<?> cqlKeyClass) {
        this.cqlKeyClass = cqlKeyClass;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    CompoundPKProperties getCompoundPKProperties() {
        return compoundPKProperties;
    }

    void setCompoundPKProperties(CompoundPKProperties compoundPKProperties) {
        this.compoundPKProperties = compoundPKProperties;
    }

    public void setIdMetaForCounterProperties(PropertyMeta idMeta) {
        counterProperties.setIdMeta(idMeta);
    }

    public CounterProperties getCounterProperties() {
        return counterProperties;
    }

    void setCounterProperties(CounterProperties counterProperties) {
        this.counterProperties = counterProperties;
    }

    Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels() {
        return consistencyLevels;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    public String getEntityClassName() {
        return entityClassName;
    }

    void setEntityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
    }

    IndexProperties getIndexProperties() {
        return indexProperties;
    }

    public void setIndexProperties(IndexProperties indexProperties) {
        this.indexProperties = indexProperties;
    }

    boolean isTimeUUID() {
        return timeUUID;
    }

    void setTimeUUID(boolean timeUUID) {
        this.timeUUID = timeUUID;
    }

    boolean isEmptyCollectionAndMapIfNull() {
        return emptyCollectionAndMapIfNull;
    }

    void setEmptyCollectionAndMapIfNull(boolean emptyCollectionAndMapIfNull) {
        this.emptyCollectionAndMapIfNull = emptyCollectionAndMapIfNull;
    }

    boolean isStaticColumn() {
        return staticColumn;
    }

    void setStaticColumn(boolean staticColumn) {
        this.staticColumn = staticColumn;
    }

    public String getCQLColumnName() {
        return cqlColumnName;
    }

    public void setCQLColumnName(String cqlColumnName) {
        this.cqlColumnName = cqlColumnName;
    }

    Codec getSimpleCodec() {
        return simpleCodec;
    }

    public void setSimpleCodec(Codec simpleCodec) {
        this.simpleCodec = simpleCodec;
    }

    ListCodec getListCodec() {
        return listCodec;
    }

    public void setListCodec(ListCodec listCodec) {
        this.listCodec = listCodec;
    }

    SetCodec getSetCodec() {
        return setCodec;
    }

    public void setSetCodec(SetCodec setCodec) {
        this.setCodec = setCodec;
    }

    MapCodec getMapCodec() {
        return mapCodec;
    }

    public void setMapCodec(MapCodec mapCodec) {
        this.mapCodec = mapCodec;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass()).add("type", type).add("entityClassName", entityClassName)
                .add("propertyName", propertyName).add("keyClass", keyClass).add("valueClass", valueClass)
                .add("counterProperties", counterProperties).add("compoundPKProperties", compoundPKProperties)
                .add("consistencyLevels", consistencyLevels).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entityClassName, propertyName, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PropertyMeta other = (PropertyMeta) obj;

        return Objects.equal(entityClassName, other.getEntityClassName())
                && Objects.equal(propertyName, other.getPropertyName()) && Objects.equal(type, other.type());
    }
}
