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
package info.archinnov.achilles.context;

import static info.archinnov.achilles.entity.metadata.EntityMeta.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.statement.prepared.CQLPreparedStatementGenerator;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class CQLDaoContextFactory {
	private static final Integer PREPARED_STATEMENT_LRU_CACHE_SIZE = 5000;
	private CQLPreparedStatementGenerator queryGenerator = new CQLPreparedStatementGenerator();

	public CQLDaoContext build(Session session, Map<Class<?>, EntityMeta> entityMetaMap, boolean hasSimpleCounter) {
		Map<Class<?>, PreparedStatement> insertPSMap = new HashMap<Class<?>, PreparedStatement>(Maps.transformValues(
				Maps.filterValues(entityMetaMap, EXCLUDE_CLUSTERED_COUNTER_FILTER), getInsertPSTransformer(session)));

		Map<Class<?>, PreparedStatement> selectEagerPSMap = new HashMap<Class<?>, PreparedStatement>(
				Maps.transformValues(entityMetaMap, getSelectEagerPSTransformer(session)));

		Map<Class<?>, Map<String, PreparedStatement>> removePSMap = new HashMap<Class<?>, Map<String, PreparedStatement>>(
				Maps.transformValues(entityMetaMap, getRemovePSTransformer(session)));

		Cache<StatementCacheKey, PreparedStatement> dynamicPSCache = CacheBuilder.newBuilder()
				.maximumSize(PREPARED_STATEMENT_LRU_CACHE_SIZE).build();

		Map<CQLQueryType, PreparedStatement> counterQueryMap;
		if (hasSimpleCounter) {
			counterQueryMap = queryGenerator.prepareSimpleCounterQueryMap(session);
		} else {
			counterQueryMap = ImmutableMap.<CQLQueryType, PreparedStatement> of();
		}

		Map<Class<?>, Map<CQLQueryType, PreparedStatement>> clusteredCounterQueriesMap = new HashMap<Class<?>, Map<CQLQueryType, PreparedStatement>>(
				Maps.transformValues(Maps.filterValues(entityMetaMap, CLUSTERED_COUNTER_FILTER),
						getClusteredCounterTransformer(session)));

		return new CQLDaoContext(insertPSMap, dynamicPSCache, selectEagerPSMap, removePSMap, counterQueryMap,
				clusteredCounterQueriesMap, session);
	}

	Function<EntityMeta, PreparedStatement> getInsertPSTransformer(final Session session) {
		return new Function<EntityMeta, PreparedStatement>() {
			@Override
			public PreparedStatement apply(EntityMeta meta) {
				return queryGenerator.prepareInsertPS(session, meta);
			}
		};
	}

	Function<EntityMeta, PreparedStatement> getSelectEagerPSTransformer(final Session session) {
		return new Function<EntityMeta, PreparedStatement>() {
			@Override
			public PreparedStatement apply(EntityMeta meta) {
				return queryGenerator.prepareSelectEagerPS(session, meta);
			}
		};
	}

	Function<EntityMeta, Map<String, PreparedStatement>> getRemovePSTransformer(final Session session) {
		return new Function<EntityMeta, Map<String, PreparedStatement>>() {
			@Override
			public Map<String, PreparedStatement> apply(EntityMeta meta) {
				return queryGenerator.prepareRemovePSs(session, meta);
			}
		};
	}

	Function<EntityMeta, Map<CQLQueryType, PreparedStatement>> getClusteredCounterTransformer(final Session session) {
		return new Function<EntityMeta, Map<CQLQueryType, PreparedStatement>>() {
			@Override
			public Map<CQLQueryType, PreparedStatement> apply(EntityMeta meta) {
				return queryGenerator.prepareClusteredCounterQueryMap(session, meta);
			}
		};
	}
}
