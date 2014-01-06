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

import static info.archinnov.achilles.internal.cql.TypeMapper.*;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;

public class RowMethodInvoker {
    private static final Logger log  = LoggerFactory.getLogger(RowMethodInvoker.class);

    public Object invokeOnRowForFields(Row row, PropertyMeta pm) {
		String propertyName = pm.getPropertyName().toLowerCase();
		Object value = null;
		if (row != null && !row.isNull(propertyName)) {
			switch (pm.type()) {
			case LIST:
				value = invokeOnRowForList(row, pm, propertyName, pm.getValueClass());
				break;
			case SET:
				value = invokeOnRowForSet(row, pm, propertyName, pm.getValueClass());
				break;
			case MAP:
				Class<?> keyClass = pm.getKeyClass();
				Class<?> valueClass = pm.getValueClass();
				value = invokeOnRowForMap(row, pm, propertyName, keyClass, valueClass);
				break;
			case ID:
			case SIMPLE:
				value = invokeOnRowForProperty(row, pm, propertyName, pm.getValueClass());
				break;
			default:
				break;
			}
		}
		return value;
	}

	public Object extractCompoundPrimaryKeyFromRow(Row row, PropertyMeta pm, boolean isManagedEntity) {
        log.trace("Extract compound primary key {} from CQL row for entity class {}",pm.getPropertyName(),pm.getEntityClassName());
		List<String> componentNames = pm.getCQLComponentNames();
		List<Class<?>> componentClasses = pm.getComponentClasses();
		List<Object> rawValues = new ArrayList<Object>(Collections.nCopies(componentNames.size(), null));

		try {
			for (Definition column : row.getColumnDefinitions()) {
				String columnName = column.getName();
				int index = componentNames.indexOf(columnName);
				Object rawValue;
				if (index >= 0) {
					Class<?> componentClass = componentClasses.get(index);
					rawValue = getRowMethod(componentClass).invoke(row, columnName);
					rawValues.set(index, rawValue);
				}
			}
			if (isManagedEntity) {
				for (int i = 0; i < componentNames.size(); i++) {
					Validator.validateNotNull(rawValues.get(i),
							"Error, the component '%s' from @EmbeddedId class '%s' cannot be found in Cassandra",
							componentNames.get(i), pm.getValueClass());
				}
			}
			return pm.decodeFromComponents(rawValues);
		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve compound primary key for entity class '"
					+ pm.getEntityClassName() + "' from CQL Row", e);
		}
	}

	private Object invokeOnRowForProperty(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract property {} from CQL row for entity class {}",propertyName,pm.getEntityClassName());
		try {
			Object rawValue = getRowMethod(valueClass).invoke(row, propertyName);
			return pm.decode(rawValue);
		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve property '" + propertyName + "' for entity class '"
					+ pm.getEntityClassName() + "' from CQL Row", e);
		}
	}

	public List<?> invokeOnRowForList(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract list property {} from CQL row for entity class {}",propertyName,pm.getEntityClassName());
		try {
			List<?> rawValues = row.getList(propertyName, toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve list property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Set<?> invokeOnRowForSet(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract set property {} from CQL row for entity class {}",propertyName,pm.getEntityClassName());
		try {
			Set<?> rawValues = row.getSet(propertyName, toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve set property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Map<?, ?> invokeOnRowForMap(Row row, PropertyMeta pm, String propertyName, Class<?> keyClass,
			Class<?> valueClass) {
        log.trace("Extract map property {} from CQL row for entity class {}",propertyName,pm.getEntityClassName());
		try {
			Map<?, ?> rawValues = row.getMap(propertyName, toCompatibleJavaType(keyClass),
					toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve map property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Object invokeOnRowForType(Row row, Class<?> type, String name) {
        log.trace("Extract property {} of type {} from CQL row ",name,type);
		try {
			return getRowMethod(type).invoke(row, name);
		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve column '" + name + "' of type '" + type.getCanonicalName()
					+ "' from CQL Row", e);
		}
	}
}
