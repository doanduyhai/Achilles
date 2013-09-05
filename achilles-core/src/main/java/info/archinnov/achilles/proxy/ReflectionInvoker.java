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
package info.archinnov.achilles.proxy;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionInvoker {
	private static final Logger log = LoggerFactory
			.getLogger(ReflectionInvoker.class);

	public Object getPrimaryKey(Object entity, PropertyMeta idMeta) {
		Method getter = idMeta.getGetter();

		log.trace("Get primary key {} from instance {} of class {}", idMeta
				.getPropertyName(), entity, getter.getDeclaringClass()
				.getCanonicalName());

		if (entity != null) {
			try {
				return getter.invoke(entity);
			} catch (Exception e) {
				throw new AchillesException(
						"Cannot get primary key value by invoking getter '"
								+ getter.getName() + "' of type '"
								+ getter.getDeclaringClass().getCanonicalName()
								+ "' from entity '" + entity + "'", e);
			}
		}
		return null;
	}

	public Object getPartitionKey(Object compoundKey, PropertyMeta idMeta) {
		if (idMeta.isEmbeddedId()) {
			Method partitionKeyGetter = idMeta.getPartitionKeyGetter();
			try {
				return partitionKeyGetter.invoke(compoundKey);
			} catch (Exception e) {
				throw new AchillesException(
						"Cannot get partition key value by invoking getter '"
								+ partitionKeyGetter.getName()
								+ "' of type '"
								+ partitionKeyGetter.getDeclaringClass()
										.getCanonicalName()
								+ "' from compoundKey '" + compoundKey + "'", e);
			}
		}
		return null;
	}

	public Object getValueFromField(Object target, Method getter) {
		log.trace("Get value with getter {} from instance {} of class {}",
				getter.getName(), target, getter.getDeclaringClass()
						.getCanonicalName());

		Object value = null;

		if (target != null) {
			try {
				value = getter.invoke(target);
			} catch (Exception e) {
				throw new AchillesException("Cannot invoke '"
						+ getter.getName() + "' of type '"
						+ getter.getDeclaringClass().getCanonicalName()
						+ "' on instance '" + target + "'", e);
			}
		}

		log.trace("Found value : {}", value);
		return value;
	}

	public void setValueToField(Object target, Method setter, Object... args) {
		log.trace(
				"Set value with setter {} to instance {} of class {} with {}",
				setter.getName(), target, setter.getDeclaringClass()
						.getCanonicalName(), args);

		if (target != null) {
			try {
				setter.invoke(target, args);
			} catch (Exception e) {
				throw new AchillesException("Cannot invoke '"
						+ setter.getName() + "'  of type '"
						+ setter.getDeclaringClass().getCanonicalName()
						+ "' on instance '" + target + "'", e);
			}
		}
	}

	public <T> T instanciate(Class<T> entityClass) {
		T newInstance;
		try {
			newInstance = entityClass.newInstance();
		} catch (Exception e) {
			throw new AchillesException(
					"Cannot instanciate entity from class '"
							+ entityClass.getCanonicalName() + "'", e);
		}
		return newInstance;
	}

	public <T> T instanciate(Constructor<T> constructor, Object... args) {
		T newInstance;
		try {
			newInstance = constructor.newInstance(args);
		} catch (Exception e) {
			throw new AchillesException(
					"Cannot instanciate entity from class '"
							+ constructor.getDeclaringClass()
									.getCanonicalName()
							+ "' with constructor '" + constructor.getName()
							+ "' and args '" + args + "'", e);
		}
		return newInstance;
	}

	public Object instanciateEmbeddedIdWithPartitionKey(PropertyMeta idMeta,
			Object partitionKey) {
		Constructor<Object> constructor = idMeta.getEmbeddedIdConstructor();
		Object newInstance;
		try {
			int parametersCount = constructor.getParameterTypes().length;
			if (parametersCount > 0) {
				Object[] constructorArgs = new Object[parametersCount];
				constructorArgs[0] = partitionKey;
				for (int i = 1; i < parametersCount; i++) {
					constructorArgs[i] = null;
				}
				newInstance = constructor.newInstance(constructorArgs);
			} else {
				newInstance = constructor.newInstance();
				setValueToField(newInstance, idMeta.getPartitionKeySetter(),
						partitionKey);
			}
		} catch (Exception e) {
			throw new AchillesException(
					"Cannot instanciate entity from class '"
							+ constructor.getDeclaringClass()
									.getCanonicalName()
							+ "' with constructor '" + constructor.getName()
							+ "' and partition key '" + partitionKey + "'", e);
		}
		return newInstance;
	}
}
