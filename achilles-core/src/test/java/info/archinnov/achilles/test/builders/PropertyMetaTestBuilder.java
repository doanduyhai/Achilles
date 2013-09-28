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
package info.archinnov.achilles.test.builders;

import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.CompoundTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.ListTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.MapTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SetTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.map.ObjectMapper;

public class PropertyMetaTestBuilder<T, K, V> {
	private EntityIntrospector achillesEntityIntrospector = new EntityIntrospector();

	private Class<T> clazz;
	private String field;
	private String entityClassName;
	private PropertyType type;
	private Class<K> keyClass;
	private Class<V> valueClass;

	private List<Class<?>> componentClasses;
	private List<String> componentNames;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	private boolean buildAccessors;
	private Class<?> idClass;
	private ObjectMapper objectMapper;
	private PropertyMeta counterIdMeta;
	private String fqcn;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
	private DataTranscoder transcoder;
	private ReflectionInvoker invoker;
	private List<String> compTimeUUID;

	public static <T, K, V> PropertyMetaTestBuilder<T, K, V> of(Class<T> clazz,
			Class<K> keyClass, Class<V> valueClass) {
		return new PropertyMetaTestBuilder<T, K, V>(clazz, keyClass, valueClass);
	}

	public static <K, V> PropertyMetaTestBuilder<CompleteBean, K, V> completeBean(
			Class<K> keyClass, Class<V> valueClass) {
		return new PropertyMetaTestBuilder<CompleteBean, K, V>(
				CompleteBean.class, keyClass, valueClass);
	}

	public static <K, V> PropertyMetaTestBuilder<Void, K, V> keyValueClass(
			Class<K> keyClass, Class<V> valueClass) {
		return new PropertyMetaTestBuilder<Void, K, V>(Void.class, keyClass,
				valueClass);
	}

	public static <V> PropertyMetaTestBuilder<Void, Void, V> valueClass(
			Class<V> valueClass) {
		return new PropertyMetaTestBuilder<Void, Void, V>(Void.class,
				Void.class, valueClass);
	}

	public PropertyMetaTestBuilder(Class<T> clazz, Class<K> keyClass,
			Class<V> valueClass) {
		this.clazz = clazz;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	public PropertyMeta build() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setType(type);
		pm.setEntityClassName(entityClassName);
		pm.setPropertyName(field);
		pm.setKeyClass(keyClass);
		pm.setValueClass(valueClass);
		pm.setIdClass(idClass);
		if (buildAccessors) {
			Field declaredField = clazz.getDeclaredField(field);
			pm.setGetter(achillesEntityIntrospector.findGetter(clazz,
					declaredField));
			Class<?> fieldClass = declaredField.getType();
			if (!Counter.class.isAssignableFrom(fieldClass)) {
				pm.setSetter(achillesEntityIntrospector.findSetter(clazz,
						declaredField));
			}
		}

		if (componentClasses != null || componentNames != null
				|| componentGetters != null || componentSetters != null) {
			EmbeddedIdProperties compoundKeyProps = new EmbeddedIdProperties();
			compoundKeyProps.setComponentClasses(componentClasses);
			compoundKeyProps.setComponentNames(componentNames);
			compoundKeyProps.setComponentGetters(componentGetters);
			compoundKeyProps.setComponentSetters(componentSetters);
			compTimeUUID = compTimeUUID != null ? compTimeUUID
					: new ArrayList<String>();
			compoundKeyProps.setTimeUUIDComponents(compTimeUUID);

			pm.setEmbeddedIdProperties(compoundKeyProps);
		}

		if (counterIdMeta != null || fqcn != null) {
			CounterProperties counterProperties = new CounterProperties(fqcn,
					counterIdMeta);
			pm.setCounterProperties(counterProperties);

		}
		objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
		if (consistencyLevels == null) {
			consistencyLevels = Pair.create(ConsistencyLevel.ONE,
					ConsistencyLevel.ONE);
		}
		pm.setConsistencyLevels(consistencyLevels);
		setTranscoder(pm);
		pm.setInvoker(invoker);
		return pm;
	}

	private void setTranscoder(PropertyMeta pm) {
		if (transcoder != null) {
			pm.setTranscoder(transcoder);
		} else if (type != null) {
			ObjectMapper objectMapper = new ObjectMapper();
			switch (type) {
			case ID:
			case SIMPLE:
			case LAZY_SIMPLE:
				pm.setTranscoder(new SimpleTranscoder(objectMapper));
				break;
			case LIST:
			case LAZY_LIST:
				pm.setTranscoder(new ListTranscoder(objectMapper));
				break;
			case SET:
			case LAZY_SET:
				pm.setTranscoder(new SetTranscoder(objectMapper));
				break;
			case MAP:
			case LAZY_MAP:
				pm.setTranscoder(new MapTranscoder(objectMapper));
				break;
			case EMBEDDED_ID:
				pm.setTranscoder(new CompoundTranscoder(objectMapper));
				break;
			default:
				break;
			}
		}
	}

	public PropertyMetaTestBuilder<T, K, V> field(String field) {
		this.field = field;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> entityClassName(
			String entityClassName) {
		this.entityClassName = entityClassName;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> type(PropertyType type) {
		this.type = type;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compClasses(
			List<Class<?>> componentClasses) {
		this.componentClasses = componentClasses;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compClasses(
			Class<?>... componentClasses) {
		this.componentClasses = Arrays.asList(componentClasses);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compNames(
			List<String> componentNames) {
		this.componentNames = componentNames;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compTimeUUID(
			String... compTimeUUIDs) {
		this.compTimeUUID = Arrays.asList(compTimeUUIDs);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compNames(String... componentNames) {
		this.componentNames = Arrays.asList(componentNames);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compGetters(
			List<Method> componentGetters) {
		this.componentGetters = componentGetters;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compGetters(
			Method... componentGetters) {
		this.componentGetters = Arrays.asList(componentGetters);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compSetters(
			List<Method> componentSetters) {
		this.componentSetters = componentSetters;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compSetters(
			Method... componentSetters) {
		this.componentSetters = Arrays.asList(componentSetters);
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

	public PropertyMetaTestBuilder<T, K, V> counterIdMeta(
			PropertyMeta counterIdMeta) {
		this.counterIdMeta = counterIdMeta;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> fqcn(String fqcn) {
		this.fqcn = fqcn;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> idClass(Class<?> idClass) {
		this.idClass = idClass;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> consistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
		this.consistencyLevels = consistencyLevels;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> transcoder(DataTranscoder transcoder) {
		this.transcoder = transcoder;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> invoker(ReflectionInvoker invoker) {
		this.invoker = invoker;
		return this;
	}
}
