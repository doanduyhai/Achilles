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
package info.archinnov.achilles.query.typed;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;

public class CQLTypedQueryValidator {

	public void validateTypedQuery(Class<?> entityClass, String queryString, EntityMeta meta) {
		PropertyMeta idMeta = meta.getIdMeta();
		String normalizedQuery = queryString.toLowerCase();

		validateRawTypedQuery(entityClass, queryString, meta);

		if (!normalizedQuery.contains("select *")) {
			if (idMeta.isEmbeddedId()) {

				for (String component : idMeta.getCQLComponentNames()) {
					Validator.validateTrue(normalizedQuery.contains(component),
							"The typed query [%s] should contain the component column '%s' for embedded id type '%s'",
							queryString, component, idMeta.getValueClass().getCanonicalName());
				}
			} else {
				String idColumn = idMeta.getPropertyName();
				Validator.validateTrue(normalizedQuery.contains(idColumn.toLowerCase()),
						"The typed query [%s] should contain the id column '%s'", queryString, idColumn);
			}
		}
	}

	public void validateRawTypedQuery(Class<?> entityClass, String queryString, EntityMeta meta) {
		String tableName = meta.getTableName().toLowerCase();
		String normalizedQuery = queryString.toLowerCase();

		Validator.validateTrue(normalizedQuery.contains(" from " + tableName),
				"The typed query [%s] should contain the ' from %s' clause if type is '%s'", queryString, tableName,
				entityClass.getCanonicalName());

	}
}
