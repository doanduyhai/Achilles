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

import static info.archinnov.achilles.cql.TypeMapper.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;

public class RowMethodInvoker {

	public Object invokeOnRowForFields(Row row, PropertyMeta pm) {
		String propertyName = pm.getPropertyName().toLowerCase();
		Object value = null;
		if (row != null && !row.isNull(propertyName)) {
			switch (pm.type()) {
			case LIST:
			case LAZY_LIST:
				value = invokeOnRowForList(row, pm, propertyName, pm.getValueClass());
				break;
			case SET:
			case LAZY_SET:
				value = invokeOnRowForSet(row, pm, propertyName, pm.getValueClass());
				break;
			case MAP:
			case LAZY_MAP:
				Class<?> keyClass = pm.getKeyClass();
				Class<?> valueClass = pm.getValueClass();
				value = invokeOnRowForMap(row, pm, propertyName, keyClass, valueClass);
				break;
			case ID:
			case SIMPLE:
			case LAZY_SIMPLE:
				value = invokeOnRowForProperty(row, pm, propertyName, pm.getValueClass());
				break;
			default:
				break;
			}
		}
		return value;
	}

	public Object extractCompoundPrimaryKeyFromRow(Row row, PropertyMeta pm, boolean isManagedEntity) {
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
		try {
			Object rawValue = getRowMethod(valueClass).invoke(row, propertyName);
			return pm.decode(rawValue);
		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve property '" + propertyName + "' for entity class '"
					+ pm.getEntityClassName() + "' from CQL Row", e);
		}
	}

	public List<?> invokeOnRowForList(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
		try {
			List<?> rawValues = row.getList(propertyName, toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve list property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Set<?> invokeOnRowForSet(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
		try {
			Set<?> rawValues = row.getSet(propertyName, toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve set property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Map<?, ?> invokeOnRowForMap(Row row, PropertyMeta pm, String propertyName, Class<?> keyClass,
			Class<?> valueClass) {
		try {
			Map<?, ?> rawValues = row.getMap(propertyName, toCompatibleJavaType(keyClass),
					toCompatibleJavaType(valueClass));
			return pm.decode(rawValues);

		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve map property '" + propertyName + "' from CQL Row", e);
		}
	}

	public Object invokeOnRowForType(Row row, Class<?> type, String name) {
		try {
			return getRowMethod(type).invoke(row, name);
		} catch (Exception e) {
			throw new AchillesException("Cannot retrieve column '" + name + "' of type '" + type.getCanonicalName()
					+ "' from CQL Row", e);
		}
	}
}
