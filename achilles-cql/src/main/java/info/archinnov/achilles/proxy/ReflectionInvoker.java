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
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.objenesis.ObjenesisStd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionInvoker {
	private static final Logger log = LoggerFactory.getLogger(ReflectionInvoker.class);

    /*
     * make property useCache of Objenesis configurable
     */
    private ObjenesisStd objenesisStd = new ObjenesisStd();

	public Object getPrimaryKey(Object entity, PropertyMeta idMeta) {
		Method getter = idMeta.getGetter();

		log.trace("Get primary key {} from instance {} of class {}", idMeta.getPropertyName(), entity, getter
				.getDeclaringClass().getCanonicalName());

		if (entity != null) {
			try {
				return getter.invoke(entity);
			} catch (Exception e) {
				throw new AchillesException("Cannot get primary key value by invoking getter '" + getter.getName()
						+ "' of type '" + getter.getDeclaringClass().getCanonicalName() + "' from entity '" + entity
						+ "'", e);
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
				throw new AchillesException("Cannot get partition key value by invoking getter '"
						+ partitionKeyGetter.getName() + "' of type '"
						+ partitionKeyGetter.getDeclaringClass().getCanonicalName() + "' from compoundKey '"
						+ compoundKey + "'", e);
			}
		}
		return null;
	}

	public Object getValueFromField(Object target, Method getter) {
		log.trace("Get value with getter {} from instance {} of class {}", getter.getName(), target, getter
				.getDeclaringClass().getCanonicalName());

		Object value = null;

		if (target != null) {
			try {
				value = getter.invoke(target);
			} catch (Exception e) {
				throw new AchillesException("Cannot invoke '" + getter.getName() + "' of type '"
						+ getter.getDeclaringClass().getCanonicalName() + "' on instance '" + target + "'", e);
			}
		}

		log.trace("Found value : {}", value);
		return value;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getListValueFromField(Object target, Method getter) {
		return (List<T>) getValueFromField(target, getter);
	}

	@SuppressWarnings("unchecked")
	public <T> Set<T> getSetValueFromField(Object target, Method getter) {
		return (Set<T>) getValueFromField(target, getter);
	}

	@SuppressWarnings("unchecked")
	public <K, V> Map<K, V> getMapValueFromField(Object target, Method getter) {
		return (Map<K, V>) getValueFromField(target, getter);
	}

	public void setValueToField(Object target, Method setter, Object args) {
		log.trace("Set value with setter {} to instance {} of class {} with {}", setter.getName(), target, setter
				.getDeclaringClass().getCanonicalName(), args);

		Class<?> parameterClass = setter.getParameterTypes()[0];
		if (parameterClass.isPrimitive()) {
			Validator.validateNotNull(args,
					"Cannot set null value to primitive type '%s' when invoking '%s' on instance of class'%s'",
					parameterClass.getCanonicalName(), setter.getName(), setter.getDeclaringClass().getCanonicalName());
		}
		if (target != null) {
			try {
				setter.invoke(target, args);
			} catch (Exception e) {
				throw new AchillesException("Cannot invoke '" + setter.getName() + "' of type '"
						+ setter.getDeclaringClass().getCanonicalName() + "' on instance '" + target + "'", e);
			}
		}
	}

	public <T> T instantiate(Class<T> entityClass) {
		log.trace("Instantiate entity class {}", entityClass);
		T newInstance;
		try {
			newInstance = entityClass.newInstance();
		} catch (Exception e) {
			throw new AchillesException(
					"Cannot instantiate entity from class '" + entityClass.getCanonicalName() + "'", e);
		}
		return newInstance;
	}

	public Object instantiateEmbeddedIdWithPartitionComponents(PropertyMeta idMeta, List<Object> partitionComponents) {
		log.trace("Instantiate entity class {} with partition key components {}", idMeta.getValueClass(),
				partitionComponents);
		Class<?> valueClass = idMeta.getValueClass();
		Object newInstance = instantiate(valueClass);
		List<Method> setters = idMeta.getPartitionComponentSetters();

		for (int i = 0; i < setters.size(); i++) {
			Method setter = setters.get(i);
			Object component = partitionComponents.get(i);
			setValueToField(newInstance, setter, component);
		}
		return newInstance;
	}

    /*
     * Warning !!!
     * Instance init code block and constructor logic
     * will not be executed when creating instance
     * with Objenesis
     */
    public <T> T instantiateImmutable(Class<T> entityClass) {
        return objenesisStd.newInstance(entityClass);
    }

    public void setValueToFinalField(Field field, Object instance, Object value) {
        if(instance != null) {
            field.setAccessible(true);
            try {
                field.set(instance,value);
            } catch (IllegalAccessException e) {
                throw new AchillesException("Cannot set value to field {} for instance {}");
            }
        }
    }
}
