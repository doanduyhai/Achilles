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

import static com.google.common.collect.Collections2.transform;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementGenerator;
import info.archinnov.achilles.query.slice.SliceQueryProperties;

public class CacheManager {
    private static final Logger log = LoggerFactory.getLogger(CacheManager.class);

    private final int maxLRUCacheSize;

    public CacheManager(int maxLRUCacheSize) {
        this.maxLRUCacheSize = maxLRUCacheSize;
    }

    private PreparedStatementGenerator generator = new PreparedStatementGenerator();

    private Function<PropertyMeta, String> propertyExtractor = new Function<PropertyMeta, String>() {
        @Override
        public String apply(PropertyMeta pm) {
            return pm.getPropertyName();
        }
    };

    public PreparedStatement getCacheForFieldSelect(Session session,Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
            PersistentStateHolder context, PropertyMeta pm) {

        log.trace("Get cache for SELECT property {} from entity class {}", pm.getPropertyName(), pm.getEntityClassName());

        Class<?> entityClass = context.getEntityClass();
        EntityMeta entityMeta = context.getEntityMeta();
        Set<String> clusteredFields = pm.forCache().extractClusteredFieldsIfNecessary();
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SELECT_FIELD, clusteredFields, entityClass, noOptions());
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareSelectField(session, entityMeta, pm);
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    public PreparedStatement getCacheForEntityInsert(Session session, Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
            PersistentStateHolder context, List<PropertyMeta> pms) {

        log.trace("Get cache for INSERT properties {} from entity class {}", pms, context.getEntityClass());

        Class<?> entityClass = context.getEntityClass();
        EntityMeta entityMeta = context.getEntityMeta();
        Set<String> fields = new HashSet<>(transform(pms, propertyExtractor));
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.INSERT, fields, entityClass, context.getOptions());
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareInsert(session, entityMeta, pms, context.getOptions());
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    public PreparedStatement getCacheForFieldsUpdate(Session session, Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
            PersistentStateHolder context, List<PropertyMeta> pms) {

        log.trace("Get cache for UPDATE properties {} from entity class {}", pms, context.getEntityClass());

        Class<?> entityClass = context.getEntityClass();
        EntityMeta entityMeta = context.getEntityMeta();
        Set<String> fields = new HashSet<>(transform(pms, propertyExtractor));
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.UPDATE_FIELDS, fields, entityClass, context.getOptions());
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareUpdateFields(session, entityMeta, pms, context.getOptions());
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    public PreparedStatement getCacheForCollectionAndMapOperation(Session session, Cache<StatementCacheKey,
            PreparedStatement> dynamicPSCache, PersistentStateHolder context, PropertyMeta pm, DirtyCheckChangeSet changeSet) {
        final Class<Object> entityClass = context.getEntityClass();
        CollectionAndMapChangeType changeType = changeSet.getChangeType();
        log.trace("Get cache for operation {} on entity class {} and property {}", changeType.name(),
                entityClass, pm.getPropertyName());

        StatementCacheKey cacheKey = new StatementCacheKey(changeType.cacheType(), Sets.newHashSet(pm.getPropertyName()), entityClass, context.getOptions());

        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareCollectionAndMapUpdate(session, context.getEntityMeta(), changeSet, context.getOptions());
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    public PreparedStatement getCacheForSliceSelectAndIterator(Session session, Cache<StatementCacheKey,PreparedStatement> dynamicPSCache,
            SliceQueryProperties sliceQueryProperties) {

        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SLICE_QUERY_SELECT, sliceQueryProperties);
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareSelectSliceQuery(session, sliceQueryProperties);
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    public PreparedStatement getCacheForSliceDelete(Session session, Cache<StatementCacheKey,PreparedStatement> dynamicPSCache,
            SliceQueryProperties sliceQueryProperties) {
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SLICE_QUERY_DELETE, sliceQueryProperties);
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null) {
            ps = generator.prepareDeleteSliceQuery(session, sliceQueryProperties);
            dynamicPSCache.put(cacheKey, ps);
            displayCacheStatistics(dynamicPSCache);
        }
        return ps;
    }

    private void displayCacheStatistics(Cache<StatementCacheKey, PreparedStatement> dynamicPSCache) {

        long cacheSize = dynamicPSCache.size();
        CacheStats cacheStats = dynamicPSCache.stats();

        log.info("Total LRU cache size {}", cacheSize);
        if (cacheSize > (maxLRUCacheSize * 0.8)) {
            log.warn("Warning, the LRU prepared statements cache is over 80% full");
        }

        if (log.isDebugEnabled()) {
            log.debug("Cache statistics :");
            log.debug("\t\t- hits count : {}", cacheStats.hitCount());
            log.debug("\t\t- hits rate : {}", cacheStats.hitRate());
            log.debug("\t\t- miss count : {}", cacheStats.missCount());
            log.debug("\t\t- miss rate : {}", cacheStats.missRate());
            log.debug("\t\t- eviction count : {}", cacheStats.evictionCount());
            log.debug("\t\t- load count : {}", cacheStats.loadCount());
            log.debug("\t\t- load success count : {}", cacheStats.loadSuccessCount());
            log.debug("\t\t- load exception count : {}", cacheStats.loadExceptionCount());
            log.debug("\t\t- total load time : {}", cacheStats.totalLoadTime());
            log.debug("\t\t- average load penalty : {}", cacheStats.averageLoadPenalty());
            log.debug("");
            log.debug("");
        }
    }
}
