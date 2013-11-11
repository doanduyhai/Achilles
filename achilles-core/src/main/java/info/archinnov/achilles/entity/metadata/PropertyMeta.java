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
package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;

public class PropertyMeta {

	private static final Function<String, String> toLowerCase = new Function<String, String>() {

		@Override
		public String apply(String input) {
			String result = null;
			if (StringUtils.isNotBlank(input))
				result = input.toLowerCase();

			return result;
		}
	};

	private PropertyType type;
	private String propertyName;
	private String entityClassName;
	private Class<?> keyClass;
	private Class<?> valueClass;
	private Method getter;
	private Method setter;
	private CounterProperties counterProperties;
	private EmbeddedIdProperties embeddedIdProperties;
	private IndexProperties indexProperties;
	private Class<?> idClass;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
	private boolean timeUUID = false;
	private DataTranscoder transcoder;
	private ReflectionInvoker invoker = new ReflectionInvoker();

	public List<Method> getComponentGetters() {
		List<Method> compGetters = new ArrayList<Method>();
		if (embeddedIdProperties != null) {
			compGetters = embeddedIdProperties.getComponentGetters();
		}
		return compGetters;
	}

	public Method getPartitionKeyGetter() {
		Method getter = null;
		if (embeddedIdProperties != null) {
			getter = embeddedIdProperties.getComponentGetters().get(0);
		}
		return getter;
	}

	public List<Method> getComponentSetters() {
		List<Method> compSetters = new ArrayList<Method>();
		if (embeddedIdProperties != null) {
			compSetters = embeddedIdProperties.getComponentSetters();
		}
		return compSetters;
	}

	public List<Class<?>> getComponentClasses() {
		List<Class<?>> compClasses = new ArrayList<Class<?>>();
		if (embeddedIdProperties != null) {
			compClasses = embeddedIdProperties.getComponentClasses();
		}
		return compClasses;
	}

	public List<String> getComponentNames() {
		List<String> components = new ArrayList<String>();
		if (embeddedIdProperties != null) {
			return embeddedIdProperties.getComponentNames();
		}
		return components;
	}

	public String getVaryingComponentNameForQuery(int fixedComponentsSize) {
		String componentName = null;
		if (embeddedIdProperties != null)
			componentName = embeddedIdProperties.getVaryingComponentNameForQuery(fixedComponentsSize);

		return componentName;
	}

	public Class<?> getVaryingComponentClassForQuery(int fixedComponentsSize) {
		Class<?> componentClass = null;
		if (embeddedIdProperties != null)
			componentClass = embeddedIdProperties.getVaryingComponentClassForQuery(fixedComponentsSize);

		return componentClass;
	}

	public List<String> getCQLComponentNames() {
		return FluentIterable.from(getComponentNames()).transform(toLowerCase).toImmutableList();
	}

	public List<String> getClusteringComponentNames() {
		return embeddedIdProperties != null ? embeddedIdProperties.getClusteringComponentNames() : Arrays
				.<String> asList();
	}

	public List<Class<?>> getClusteringComponentClasses() {
		return embeddedIdProperties != null ? embeddedIdProperties.getClusteringComponentClasses() : Arrays
				.<Class<?>> asList();
	}

	public List<String> getPartitionComponentNames() {
		return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentNames() : Arrays
				.<String> asList();
	}

	public List<Class<?>> getPartitionComponentClasses() {
		return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentClasses() : Arrays
				.<Class<?>> asList(valueClass);
	}

	public List<Object> extractPartitionComponents(List<Object> components) {
		return embeddedIdProperties != null ? embeddedIdProperties.extractPartitionComponents(components) : Arrays
				.asList();
	}

	public List<Object> extractPartitionComponents(Object primaryKey) {
		List<Object> partitionComponents;
		if (isCompositePartitionKey()) {
			partitionComponents = encodeToComponents(primaryKey);
			partitionComponents = extractPartitionComponents(partitionComponents);
		} else if (isEmbeddedId()) {
			partitionComponents = Arrays.<Object> asList(getPartitionKey(primaryKey));
		} else {
			partitionComponents = Arrays.<Object> asList(primaryKey);
		}

		return partitionComponents;
	}

	public void validatePartitionComponents(List<Object> partitionComponents) {
		if (embeddedIdProperties != null) {
			embeddedIdProperties.validatePartitionComponents(this.entityClassName, partitionComponents);
		}
	}

	public void validateClusteringComponents(List<Object> clusteringComponents) {
		if (embeddedIdProperties != null) {
			embeddedIdProperties.validateClusteringComponents(this.entityClassName, clusteringComponents);
		}
	}

	public List<Method> getPartitionComponentSetters() {
		return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentSetters() : Arrays
				.<Method> asList();
	}

	public List<Object> extractClusteringComponents(List<Object> components) {
		return embeddedIdProperties != null ? embeddedIdProperties.extractClusteringComponents(components) : Arrays
				.asList();

	}

	public boolean isComponentTimeUUID(String componentName) {
		return embeddedIdProperties != null && embeddedIdProperties.getTimeUUIDComponents().contains(componentName);
	}

	public String getOrderingComponent() {
		String component = null;
		if (embeddedIdProperties != null) {
			return embeddedIdProperties.getOrderingComponent();
		}
		return component;
	}

	public String getReversedComponent() {
		String component = null;
		if (embeddedIdProperties != null) {
			return embeddedIdProperties.getReversedComponent();
		}
		return component;
	}

	public boolean hasReversedComponent() {
		if (embeddedIdProperties != null) {
			return embeddedIdProperties.hasReversedComponent();
		}
		return false;
	}

	public PropertyMeta counterIdMeta() {
		return counterProperties != null ? counterProperties.getIdMeta() : null;
	}

	public String fqcn() {
		return counterProperties != null ? counterProperties.getFqcn() : null;
	}

	public boolean isLazy() {
		return this.type.isLazy();
	}

	public boolean isCounter() {
		return this.type.isCounter();
	}

	public boolean isEmbeddedId() {
		return type.isEmbeddedId();
	}

	public boolean isCompositePartitionKey() {
		return embeddedIdProperties != null && embeddedIdProperties.isCompositePartitionKey();
	}

	public ConsistencyLevel getReadConsistencyLevel() {
		return consistencyLevels != null ? consistencyLevels.left : null;
	}

	public ConsistencyLevel getWriteConsistencyLevel() {
		return consistencyLevels != null ? consistencyLevels.right : null;
	}

	public Object decode(Object cassandraValue) {
		return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
	}

	public Object decodeKey(Object cassandraValue) {
		return cassandraValue == null ? null : transcoder.decodeKey(this, cassandraValue);
	}

	public List<Object> decode(List<?> cassandraValue) {
		return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
	}

	public Set<Object> decode(Set<?> cassandraValue) {
		return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
	}

	public Map<Object, Object> decode(Map<?, ?> cassandraValue) {
		return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
	}

	public Object decodeFromComponents(List<?> components) {
		return components == null ? null : transcoder.decodeFromComponents(this, components);
	}

	public Object encode(Object entityValue) {
		return entityValue == null ? null : transcoder.encode(this, entityValue);
	}

	public Object encodeKey(Object entityValue) {
		return entityValue == null ? null : transcoder.encodeKey(this, entityValue);
	}

	public <T> Object encode(List<T> entityValue) {
		return entityValue == null ? null : transcoder.encode(this, entityValue);
	}

	public Set<Object> encode(Set<?> entityValue) {
		return entityValue == null ? null : transcoder.encode(this, entityValue);
	}

	public Map<Object, Object> encode(Map<?, ?> entityValue) {
		return entityValue == null ? null : transcoder.encode(this, entityValue);
	}

	public List<Object> encodeToComponents(Object compoundKey) {
		return compoundKey == null ? null : transcoder.encodeToComponents(this, compoundKey);
	}

	public List<Object> encodeToComponents(List<Object> components) {
		return components == null ? null : transcoder.encodeToComponents(this, components);
	}

	public String forceEncodeToJSON(Object object) {
		return transcoder.forceEncodeToJSON(object);
	}

	public Object forceDecodeFromJSON(String cassandraValue, Class<?> targetType) {
		return transcoder.forceDecodeFromJSON(cassandraValue, targetType);
	}

	public Object forceDecodeFromJSON(String cassandraValue) {
		return transcoder.forceDecodeFromJSON(cassandraValue, valueClass);
	}

	public Object getPrimaryKey(Object entity) {
		if (type.isId()) {
			return invoker.getPrimaryKey(entity, this);
		} else {
			throw new IllegalStateException("Cannot get primary key on a non id field '" + propertyName + "'");
		}
	}

	public Object getPartitionKey(Object compoundKey) {
		if (type.isEmbeddedId()) {
			return invoker.getPartitionKey(compoundKey, this);
		} else {
			throw new IllegalStateException("Cannot get partition key on a non embedded id field '" + propertyName
					+ "'");
		}
	}

	public Object instanciate() {
		return invoker.instanciate(valueClass);
	}

	Object instanciateEmbeddedIdWithPartitionKey(List<Object> partitionComponents) {
		if (type.isEmbeddedId()) {
			return invoker.instanciateEmbeddedIdWithPartitionComponents(this, partitionComponents);
		} else {
			throw new IllegalStateException(
					"Cannot instanciate embedded id with partition key on a non embedded id field '" + propertyName
							+ "'");
		}
	}

	public Object getValueFromField(Object target) {
		return invoker.getValueFromField(target, getter);
	}

	public List<?> getListValueFromField(Object target) {
		return invoker.getListValueFromField(target, getter);
	}

	public Set<?> getSetValueFromField(Object target) {
		return invoker.getSetValueFromField(target, getter);
	}

	public Map<?, ?> getMapValueFromField(Object target) {
		return invoker.getMapValueFromField(target, getter);
	}

	public void setValueToField(Object target, Object args) {
		invoker.setValueToField(target, setter, args);
	}

	public Class<?> getValueClassForTableCreation() {
		if (timeUUID) {
			return InternalTimeUUID.class;
		} else {
			return valueClass;
		}
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

	public Class<?> getKeyClass() {
		return keyClass;
	}

	public void setKeyClass(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	public Class<?> getValueClass() {
		return valueClass;
	}

	public void setValueClass(Class<?> valueClass) {
		this.valueClass = valueClass;
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

	public EmbeddedIdProperties getEmbeddedIdProperties() {
		return embeddedIdProperties;
	}

	public void setEmbeddedIdProperties(EmbeddedIdProperties embeddedIdProperties) {
		this.embeddedIdProperties = embeddedIdProperties;
	}

	public Class<?> getIdClass() {
		return idClass;
	}

	public void setIdClass(Class<?> idClass) {
		this.idClass = idClass;
	}

	public CounterProperties getCounterProperties() {
		return counterProperties;
	}

	public void setCounterProperties(CounterProperties counterProperties) {
		this.counterProperties = counterProperties;
	}

	public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
		this.consistencyLevels = consistencyLevels;
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName) {
		this.entityClassName = entityClassName;
	}

	public boolean isIndexed() {
		return this.indexProperties != null;
	}

	public IndexProperties getIndexProperties() {
		return indexProperties;
	}

	public void setIndexProperties(IndexProperties indexProperties) {
		this.indexProperties = indexProperties;
	}

	public DataTranscoder getTranscoder() {
		return transcoder;
	}

	public void setTranscoder(DataTranscoder transcoder) {
		this.transcoder = transcoder;
	}

	public ReflectionInvoker getInvoker() {
		return invoker;
	}

	public void setInvoker(ReflectionInvoker invoker) {
		this.invoker = invoker;
	}

	public boolean isTimeUUID() {
		return timeUUID;
	}

	public void setTimeUUID(boolean timeUUID) {
		this.timeUUID = timeUUID;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("type", type).add("entityClassName", entityClassName)
				.add("propertyName", propertyName).add("keyClass", keyClass).add("valueClass", valueClass)
				.add("counterProperties", counterProperties).add("embeddedIdProperties", embeddedIdProperties)
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
