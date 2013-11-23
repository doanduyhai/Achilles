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
package info.archinnov.achilles.statement.cache;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.statement.prepared.PreparedStatementGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

public class CacheManager {
	private PreparedStatementGenerator generator = new PreparedStatementGenerator();

	private Function<PropertyMeta, String> propertyExtractor = new Function<PropertyMeta, String>() {
		@Override
		public String apply(PropertyMeta pm) {
			return pm.getPropertyName();
		}
	};

	public PreparedStatement getCacheForFieldSelect(Session session,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache, PersistenceContext context, PropertyMeta pm) {
		Class<?> entityClass = context.getEntityClass();
		EntityMeta entityMeta = context.getEntityMeta();
		Set<String> clusteredFields = extractClusteredFieldsIfNecessary(pm);
		StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SELECT_FIELD, entityMeta.getTableName(),
				clusteredFields, entityClass);
		PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
		if (ps == null) {
			ps = generator.prepareSelectFieldPS(session, entityMeta, pm);
			dynamicPSCache.put(cacheKey, ps);
		}
		return ps;
	}

	public PreparedStatement getCacheForFieldsUpdate(Session session,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache, PersistenceContext context,
			List<PropertyMeta> pms) {
		Class<?> entityClass = context.getEntityClass();
		EntityMeta entityMeta = context.getEntityMeta();
		Set<String> fields = new HashSet<String>(Collections2.transform(pms, propertyExtractor));
		StatementCacheKey cacheKey = new StatementCacheKey(CacheType.UPDATE_FIELDS, entityMeta.getTableName(), fields,
				entityClass);
		PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
		if (ps == null) {
			ps = generator.prepareUpdateFields(session, entityMeta, pms);
			dynamicPSCache.put(cacheKey, ps);
		}
		return ps;
	}

	private Set<String> extractClusteredFieldsIfNecessary(PropertyMeta pm) {
		if (pm.isEmbeddedId()) {
			return new HashSet<String>(pm.getComponentNames());
		} else {
			return Sets.newHashSet(pm.getPropertyName());
		}
	}
}
