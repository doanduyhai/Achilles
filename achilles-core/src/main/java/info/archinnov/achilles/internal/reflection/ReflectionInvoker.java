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
package info.archinnov.achilles.internal.reflection;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionInvoker {
	private static final Logger log = LoggerFactory.getLogger(ReflectionInvoker.class);

    private FieldAccessor accessor = new FieldAccessor();
    private ObjectInstantiator instantiator = new ObjectInstantiator();

    public Object getPrimaryKey(Object entity, PropertyMeta idMeta) {

        final Field field = idMeta.getField();

        log.trace("Get primary key {} from instance {} of class {}", idMeta.getPropertyName(), entity, field
                .getDeclaringClass().getCanonicalName());

        if (entity != null) {
            try {
                return accessor.getValueFromField(field, entity);
            } catch (IllegalAccessException | IllegalArgumentException e) {
                throw new AchillesException("Cannot get primary key from field '" + field.getName()
                                                    + "' of type '" + field.getDeclaringClass().getCanonicalName() + "' from entity '" + entity
                                                    + "'", e);
            }
        }
        return null;
    }

    public Object getPartitionKey(Object compoundKey, PropertyMeta idMeta) {
        if (idMeta.isEmbeddedId()) {
            final Field partitionKeyField = idMeta.getPartitionKeyField();
            try {
                return accessor.getValueFromField(partitionKeyField, compoundKey);
            } catch (IllegalAccessException | IllegalArgumentException e) {
                throw new AchillesException("Cannot get partition key from field '" + partitionKeyField.getName()
                                                    + "' of type '" + partitionKeyField.getDeclaringClass().getCanonicalName()
                                                    + "' from compoundKey '" + compoundKey + "'", e);
            }
        }
        return null;
    }

    public <T> T getValueFromField(Object target, Field field) {
        log.trace("Get value from field {} from instance {} of class {}", field.getName(), target, field
                .getDeclaringClass().getCanonicalName());

        T value = null;

        if (target != null) {
            try {
                value = accessor.getValueFromField(field, target);
            } catch (IllegalAccessException | IllegalArgumentException e) {
                throw new AchillesException("Cannot get value from field '" + field.getName() + "' of type '"
                                                    + field.getDeclaringClass().getCanonicalName() + "' on instance '" + target + "'", e);
            }
        }
        log.trace("Found value : {}", value);
        return value;
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


    public <T> List<T> getListValueFromField(Object target, Field field) {
        return getValueFromField(target, field);
    }

    public <T> Set<T> getSetValueFromField(Object target, Field field) {
        return getValueFromField(target, field);
    }

    public <K, V> Map<K, V> getMapValueFromField(Object target, Field field) {
        return getValueFromField(target, field);
    }

    public void setValueToField(Object target, Field field, Object args) {
        log.trace("Set value to field {} from instance {} of class {} with {}", field.getName(), target, field
                .getDeclaringClass().getCanonicalName(), args);

        final Class<?> type = field.getType();
        if (type.isPrimitive()) {
            Validator.validateNotNull(args,
                                      "Cannot set null value to primitive type '%s' of field '%s' on instance of class'%s'",
                                      type.getCanonicalName(), field.getName(), field.getDeclaringClass().getCanonicalName());
        }
        if (target != null) {
            try {
                accessor.setValueToField(field, target, args);
            } catch (IllegalAccessException | IllegalArgumentException e) {
                throw new AchillesException("Cannot set value to field '" + field.getName() + "' of type '"
                                                    + field.getDeclaringClass().getCanonicalName() + "' on instance '" + target + "'", e);
            }
        }
    }

    public <T> T instantiate(Class<T> entityClass) {
        log.trace("Instantiate entity class {}", entityClass);
        return instantiator.instantiate(entityClass);
    }

	public Object instantiateEmbeddedIdWithPartitionComponents(PropertyMeta idMeta, List<Object> partitionComponents) {
		log.trace("Instantiate entity class {} with partition key components {}", idMeta.getValueClass(),
				partitionComponents);
		Class<?> valueClass = idMeta.getValueClass();
		Object newInstance = instantiate(valueClass);
		List<Field> fields = idMeta.getPartitionComponentFields();

		for (int i = 0; i < fields.size(); i++) {
			Field field = fields.get(i);
			Object component = partitionComponents.get(i);
			setValueToField(newInstance, field, component);
		}
		return newInstance;
	}
}
