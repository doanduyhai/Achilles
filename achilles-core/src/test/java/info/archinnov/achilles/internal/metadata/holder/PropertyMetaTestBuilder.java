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

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.parsing.EntityIntrospector;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;

public class PropertyMetaTestBuilder<T, K, V> {
    private EntityIntrospector achillesEntityIntrospector = new EntityIntrospector();

    private EmbeddedIdPropertiesBuilder partitionBuilder = new EmbeddedIdPropertiesBuilder();
    private EmbeddedIdPropertiesBuilder clusteringBuilder = new EmbeddedIdPropertiesBuilder();

    private Class<T> clazz;
    private String propertyName;
    private String cqlColumnName;
    private String entityClassName;
    private PropertyType type;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private Class<?> cqlKeyClass;
    private Class<?> cqlValueClass;


    private boolean buildAccessors;
    private ObjectMapper objectMapper;
    private PropertyMeta counterIdMeta;
    private String fqcn;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private ReflectionInvoker invoker;
    private boolean staticColumn = false;
    private String indexName;

    public static <T, K, V> PropertyMetaTestBuilder<T, K, V> of(Class<T> clazz, Class<K> keyClass, Class<V> valueClass) {
        return new PropertyMetaTestBuilder<>(clazz, keyClass, valueClass);
    }

    public static <K, V> PropertyMetaTestBuilder<CompleteBean, K, V> completeBean(Class<K> keyClass, Class<V> valueClass) {
        return new PropertyMetaTestBuilder<>(CompleteBean.class, keyClass, valueClass);
    }

    public static <K, V> PropertyMetaTestBuilder<Void, K, V> keyValueClass(Class<K> keyClass, Class<V> valueClass) {
        return new PropertyMetaTestBuilder<>(Void.class, keyClass, valueClass);
    }

    public static <V> PropertyMetaTestBuilder<Void, Void, V> valueClass(Class<V> valueClass) {
        return new PropertyMetaTestBuilder<>(Void.class, Void.class, valueClass);
    }

    public PropertyMetaTestBuilder(Class<T> clazz, Class<K> keyClass, Class<V> valueClass) {
        this.clazz = clazz;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    public PropertyMeta build() throws Exception {
        PropertyMeta pm = new PropertyMeta();
        pm.setType(type);
        pm.setEntityClassName(entityClassName);
        pm.setPropertyName(propertyName);
        pm.setCQLColumnName(cqlColumnName);
        pm.setKeyClass(keyClass);
        pm.setValueClass(valueClass);
        pm.setCQLKeyClass(fromNullable(cqlKeyClass).or((Class) keyClass));
        pm.setCqlValueClass(fromNullable(cqlValueClass).or((Class) valueClass));

        if (StringUtils.isNotBlank(propertyName) && clazz == CompleteBean.class) {
            pm.setField(clazz.getDeclaredField(propertyName));
        }

        if (buildAccessors) {
            Field declaredField = clazz.getDeclaredField(propertyName);
            pm.setGetter(achillesEntityIntrospector.findGetter(clazz, declaredField));
            Class<?> fieldClass = declaredField.getType();
            if (!Counter.class.isAssignableFrom(fieldClass)) {
                pm.setSetter(achillesEntityIntrospector.findSetter(clazz, declaredField));
            }
        }

        if (!partitionBuilder.getPropertyMetas().isEmpty()) {
            pm.setEmbeddedIdProperties(EmbeddedIdPropertiesBuilder.buildEmbeddedIdProperties(partitionBuilder.buildPartitionKeys(), clusteringBuilder.buildClusteringKeys(), entityClassName));
        }

        if (counterIdMeta != null || fqcn != null) {
            CounterProperties counterProperties = new CounterProperties(fqcn, counterIdMeta);
            pm.setCounterProperties(counterProperties);

        }
        objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        if (consistencyLevels == null) {
            consistencyLevels = Pair.create(ConsistencyLevel.ONE, ConsistencyLevel.ONE);
        }
        pm.setConsistencyLevels(consistencyLevels);
//        pm.setInvoker(invoker);
        pm.setStaticColumn(staticColumn);
        if (StringUtils.isNotBlank(indexName)) {
            pm.setIndexProperties(new IndexProperties(indexName, propertyName));
        }
        return pm;
    }


    public PropertyMetaTestBuilder<T, K, V> propertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> cqlColumnName(String cqlColumnName) {
        this.cqlColumnName = cqlColumnName;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> entityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> type(PropertyType type) {
        this.type = type;
        return this;
    }

    public PropertyMetaTestBuilder<T,K,V> withPartitionMeta(String name, Class<?> type) throws Exception {
        this.partitionBuilder.addPropertyMeta(PropertyMetaTestBuilder.valueClass(type).propertyName(name).build());
        return this;
    }

    public PropertyMetaTestBuilder<T,K,V> withClusteringMeta(String name, Class<?> type) throws Exception {
        this.clusteringBuilder.addPropertyMeta(PropertyMetaTestBuilder.valueClass(type).propertyName(name).build());
        return this;
    }


    public PropertyMetaTestBuilder<T, K, V> partitionKeyMetas(PropertyMeta... propertyMetas) {
        for (PropertyMeta propertyMeta : propertyMetas) {
            this.partitionBuilder.addPropertyMeta(propertyMeta);
        }
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> clusteringKeyMetas(PropertyMeta... propertyMetas) {
        for (PropertyMeta propertyMeta : propertyMetas) {
            this.clusteringBuilder.addPropertyMeta(propertyMeta);
        }
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> clusteringOrders(ClusteringOrder...clusteringOrders) {
        this.clusteringBuilder.setClusteringOrders(Arrays.asList(clusteringOrders));
        return this;
    }


    public PropertyMetaTestBuilder<T, K, V> accessors() {
        this.buildAccessors = true;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> mapper(ObjectMapper mapper) {
        this.objectMapper = mapper;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> counterIdMeta(PropertyMeta counterIdMeta) {
        this.counterIdMeta = counterIdMeta;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> fqcn(String fqcn) {
        this.fqcn = fqcn;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> consistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> invoker(ReflectionInvoker invoker) {
        this.invoker = invoker;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> staticColumn() {
        this.staticColumn = true;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> staticColumn(boolean staticColumn) {
        this.staticColumn = staticColumn;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> indexed(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> indexed() {
        this.indexName = propertyName;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> cqlKeyType(Class<?> cqlKeyClass) {
        this.cqlKeyClass = cqlKeyClass;
        return this;
    }

    public PropertyMetaTestBuilder<T, K, V> cqlValueType(Class<?> cqlValueClass) {
        this.cqlValueClass = cqlValueClass;
        return this;
    }
}
