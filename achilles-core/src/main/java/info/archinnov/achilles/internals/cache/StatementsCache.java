/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.cache;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static java.lang.String.format;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;

import info.archinnov.achilles.exception.AchillesException;

public class StatementsCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatementsCache.class);

    private final Cache<String, PreparedStatement> dynamicCache;
    private final Cache<CacheKey, PreparedStatement> staticCache;
    private final int maxLRUCacheSize;


    public StatementsCache(int maxLRUCacheSize) {
        this.maxLRUCacheSize = maxLRUCacheSize;
        this.dynamicCache = newBuilder().maximumSize(maxLRUCacheSize).build();
        this.staticCache = newBuilder().build();
    }

    public void putStaticCache(CacheKey cacheKey, Callable<PreparedStatement> psSupplier) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Putting static cache for key %s", cacheKey));
            }
            staticCache.get(cacheKey, psSupplier);
        } catch (ExecutionException e) {
            throw new AchillesException(e);
        }
    }

    public PreparedStatement getStaticCache(CacheKey cacheKey) {
        final PreparedStatement preparedStatement = staticCache.getIfPresent(cacheKey);
        if (preparedStatement == null) {
            throw new AchillesException(format("Cannot find static cached prepared statement for cache key %s", cacheKey));
        }
        return preparedStatement;
    }

    public PreparedStatement getDynamicCache(final String queryString, Session session) {
        AtomicBoolean displayStats = new AtomicBoolean(false);
        try {
            final PreparedStatement preparedStatement = dynamicCache.get(queryString, () -> {
                displayStats.getAndSet(true);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Preparing dynamic query : " + queryString);
                }
                return session.prepare(queryString);
            });

            if (displayStats.get()) displayCacheStatistics();
            return preparedStatement;
        } catch (ExecutionException e) {
            throw new AchillesException(e);
        }
    }

    private void displayCacheStatistics() {

        long cacheSize = dynamicCache.size();
        CacheStats cacheStats = dynamicCache.stats();

        LOGGER.info("Total LRU cache size {}", cacheSize);
        if (cacheSize > (maxLRUCacheSize * 0.8)) {
            LOGGER.warn("Warning, the LRU prepared statements cache is over 80% full");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Cache statistics :");
            LOGGER.debug("\t\t- hits count : {}", cacheStats.hitCount());
            LOGGER.debug("\t\t- hits rate : {}", cacheStats.hitRate());
            LOGGER.debug("\t\t- miss count : {}", cacheStats.missCount());
            LOGGER.debug("\t\t- miss rate : {}", cacheStats.missRate());
            LOGGER.debug("\t\t- eviction count : {}", cacheStats.evictionCount());
            LOGGER.debug("\t\t- load count : {}", cacheStats.loadCount());
            LOGGER.debug("\t\t- load success count : {}", cacheStats.loadSuccessCount());
            LOGGER.debug("\t\t- load exception count : {}", cacheStats.loadExceptionCount());
            LOGGER.debug("\t\t- total load time : {}", cacheStats.totalLoadTime());
            LOGGER.debug("\t\t- average load penalty : {}", cacheStats.averageLoadPenalty());
            LOGGER.debug("");
            LOGGER.debug("");
        }
    }

}
