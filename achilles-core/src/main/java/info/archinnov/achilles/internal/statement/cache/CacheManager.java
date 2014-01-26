/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.statement.cache;

import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

public class CacheManager {
    private static final Logger log  = LoggerFactory.getLogger(CacheManager.class);

    private PreparedStatementGenerator generator = new PreparedStatementGenerator();

	private Function<PropertyMeta, String> propertyExtractor = new Function<PropertyMeta, String>() {
		@Override
		public String apply(PropertyMeta pm) {
			return pm.getPropertyName();
		}
	};

	public PreparedStatement getCacheForFieldSelect(Session session,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache, PersistenceContext context, PropertyMeta pm) {

        log.trace("Get cache for SELECT property {} from entity class {}",pm.getPropertyName(),pm.getEntityClassName());

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

        log.trace("Get cache for UPDATE properties {} from entity class {}",pms,context.getEntityClass());

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
