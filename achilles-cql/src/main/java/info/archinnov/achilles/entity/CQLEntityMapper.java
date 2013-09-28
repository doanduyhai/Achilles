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
package info.archinnov.achilles.entity;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.EntityMapper;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;

import java.util.Map;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;

public class CQLEntityMapper extends EntityMapper {

	private CQLRowMethodInvoker cqlRowInvoker = new CQLRowMethodInvoker();

	public void setEagerPropertiesToEntity(Row row, EntityMeta entityMeta,
			Object entity) {
		for (PropertyMeta pm : entityMeta.getEagerMetas()) {
			setPropertyToEntity(row, pm, entity);
		}
	}

	public void setPropertyToEntity(Row row, PropertyMeta pm, Object entity) {
		if (row != null) {
			if (pm.isEmbeddedId()) {
				Object compoundKey = cqlRowInvoker
						.extractCompoundPrimaryKeyFromRow(row, pm, true);
				pm.setValueToField(entity, compoundKey);
			} else {
				String propertyName = pm.getPropertyName();
				if (!row.isNull(propertyName)) {
					Object value = cqlRowInvoker.invokeOnRowForFields(row, pm);
					pm.setValueToField(entity, value);
				}
			}
		}
	}

	public <T> T mapRowToEntityWithPrimaryKey(Class<T> entityClass,
			EntityMeta meta, Row row, Map<String, PropertyMeta> propertiesMap,
			boolean isEntityManaged) {
		T entity = null;
		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
		if (columnDefinitions != null) {
			entity = meta.<T> instanciate();
			for (Definition column : columnDefinitions) {
				String columnName = column.getName();
				PropertyMeta pm = propertiesMap.get(columnName);
				if (pm != null) {
					Object value = cqlRowInvoker.invokeOnRowForFields(row, pm);
					pm.setValueToField(entity, value);
				}
			}
			PropertyMeta idMeta = meta.getIdMeta();
			if (idMeta.isEmbeddedId()) {
				Object compoundKey = cqlRowInvoker
						.extractCompoundPrimaryKeyFromRow(row, idMeta,
								isEntityManaged);
				idMeta.setValueToField(entity, compoundKey);
			}
		}
		return entity;
	}
}
