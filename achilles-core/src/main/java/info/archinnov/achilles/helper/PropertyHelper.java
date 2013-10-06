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
package info.archinnov.achilles.helper;

import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyHelper {
	private static final Logger log = LoggerFactory.getLogger(PropertyHelper.class);

	public static Set<Class<?>> allowedTypes = new HashSet<Class<?>>();
	protected EntityIntrospector entityIntrospector = new EntityIntrospector();

	static {
		// Bytes
		allowedTypes.add(byte[].class);
		allowedTypes.add(ByteBuffer.class);

		// Boolean
		allowedTypes.add(Boolean.class);
		allowedTypes.add(boolean.class);

		// Date
		allowedTypes.add(Date.class);

		// Double
		allowedTypes.add(Double.class);
		allowedTypes.add(double.class);

		// Char
		allowedTypes.add(Character.class);

		// Float
		allowedTypes.add(Float.class);
		allowedTypes.add(float.class);

		// Integer
		allowedTypes.add(BigInteger.class);
		allowedTypes.add(Integer.class);
		allowedTypes.add(int.class);

		// Long
		allowedTypes.add(Long.class);
		allowedTypes.add(long.class);

		// Short
		allowedTypes.add(Short.class);
		allowedTypes.add(short.class);

		// String
		allowedTypes.add(String.class);

		// UUID
		allowedTypes.add(UUID.class);

	}

	public PropertyHelper() {
	}

	public <T> Class<T> inferValueClassForListOrSet(Type genericType, Class<?> entityClass) {
		log.debug("Infer parameterized value class for collection type {} of entity class {} ", genericType.toString(),
				entityClass.getCanonicalName());

		Class<T> valueClass;
		if (genericType instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0) {
				Type type = actualTypeArguments[actualTypeArguments.length - 1];
				valueClass = getClassFromType(type);
			} else {
				throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
						+ "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
			}
		} else {
			throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
					+ "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
		}

		log.trace("Inferred value class : {}", valueClass.getCanonicalName());

		return valueClass;
	}

	public boolean isLazy(Field field) {
		log.debug("Check @Lazy annotation on field {} of class {}", field.getName(), field.getDeclaringClass()
				.getCanonicalName());

		boolean lazy = false;
		if (field.getAnnotation(Lazy.class) != null) {
			lazy = true;
		}
		return lazy;
	}

	public boolean isIndexed(Field field) {
		log.debug("Check @Index annotation on field {} of class {}", field.getName(), field.getDeclaringClass()
				.getCanonicalName());

		boolean indexed = false;
		if (field.getAnnotation(Index.class) != null) {
			indexed = true;
		}
		return indexed;
	}

	public boolean hasConsistencyAnnotation(Field field) {
		log.debug("Check @Consistency annotation on field {} of class {}", field.getName(), field.getDeclaringClass()
				.getCanonicalName());

		boolean consistency = false;
		if (field.getAnnotation(Consistency.class) != null) {
			consistency = true;
		}
		return consistency;
	}

	public static <T> boolean isSupportedType(Class<T> valueClass) {
		return allowedTypes.contains(valueClass);
	}

	public <T> Pair<ConsistencyLevel, ConsistencyLevel> findConsistencyLevels(Field field,
			AchillesConsistencyLevelPolicy policy) {
		log.debug("Find consistency configuration for field {} of class {}", field.getName(), field.getDeclaringClass()
				.getCanonicalName());

		Consistency clevel = field.getAnnotation(Consistency.class);

		ConsistencyLevel defaultGlobalRead = entityIntrospector.getDefaultGlobalReadConsistency(policy);
		ConsistencyLevel defaultGlobalWrite = entityIntrospector.getDefaultGlobalWriteConsistency(policy);

		if (clevel != null) {
			defaultGlobalRead = clevel.read();
			defaultGlobalWrite = clevel.write();
		}

		log.trace("Found consistency levels : {} / {}", defaultGlobalRead, defaultGlobalWrite);
		return Pair.create(defaultGlobalRead, defaultGlobalWrite);
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> getClassFromType(Type type) {
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return (Class<T>) parameterizedType.getRawType();
		} else if (type instanceof Class) {
			return (Class<T>) type;
		} else {
			throw new IllegalArgumentException("Cannot determine java class of type '" + type + "'");
		}
	}
}
