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
package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftJoinEntityLoader {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftJoinEntityLoader.class);

	private ThriftEntityMapper mapper = new ThriftEntityMapper();

	public Map<Object, Object> loadJoinEntities(Class<?> entityClass,
			List<Object> keys, EntityMeta entityMeta,
			ThriftGenericEntityDao joinEntityDao) {
		if (log.isTraceEnabled()) {
			log.trace("Load join entities of class {} with primary keys {}",
					entityClass.getCanonicalName(), StringUtils.join(keys, ","));
		}

		Validator.validateNotNull(entityClass,
				"Entity class should not be null");
		Validator.validateNotEmpty(keys,
				"List of join primary keys '%s' should not be empty", keys);
		Validator.validateNotNull(entityMeta,
				"Entity meta for '%s' should not be null",
				entityClass.getCanonicalName());

		Map<Object, Object> entitiesByKey = new HashMap<Object, Object>();
		Map<Object, List<Pair<Composite, String>>> rows = joinEntityDao
				.eagerFetchEntities(keys);

		for (Entry<Object, List<Pair<Composite, String>>> entry : rows
				.entrySet()) {
			Object entity = entityMeta.<Object> instanciate();

			Object key = entry.getKey();
			List<Pair<Composite, String>> columns = entry.getValue();
			if (columns.size() > 0) {
				mapper.setEagerPropertiesToEntity(key, columns, entityMeta,
						entity);
				entityMeta.getIdMeta().setValueToField(entity, key);
				entitiesByKey.put(key, entity);
			}
		}
		return entitiesByKey;
	}
}
