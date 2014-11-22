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

package info.archinnov.achilles.query.slice;

import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import static info.archinnov.achilles.query.slice.BoundingMode.*;
import static info.archinnov.achilles.query.slice.OrderingMode.ASCENDING;
import static info.archinnov.achilles.query.slice.OrderingMode.DESCENDING;
import static info.archinnov.achilles.query.slice.SliceQueryProperties.SliceType;

public abstract class AsyncSelectPartitionRoot<TYPE, T extends AsyncSelectPartitionRoot<TYPE,T>> extends SliceQueryRootExtended<TYPE, T> {

    protected AsyncSelectPartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    /**
     *
     * Get selected entities asynchronously without filtering clustering keys. If no limit has been set, the default LIMIT 100 applies
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .get();
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 100
     *
     * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
     */
    public AchillesFuture<List<TYPE>> get() {
        return super.asyncGetInternal();
    }

    /**
     *
     * Get selected entities asynchronously without filtering clustering keys using provided limit
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .get(23);
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC <strong>LIMIT 23</strong>
     *
     * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
     */
    public AchillesFuture<List<TYPE>> get(int limit) {
        super.properties.limit(limit);
        return super.asyncGetInternal();
    }

    /**
     *
     * Get first entity asynchronously without filtering clustering keys
     * To get the last entity, just use getOne() with orderByDescending()
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .getOne();
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC <strong>LIMIT 1</strong>
     *
     * @return AchillesFuture&lt;TYPE&gt;
     */
    public AchillesFuture<TYPE> getOne() {
        super.properties.limit(1);
        return super.asyncGetOneInternal();
    }

    /**
     *
     * Get entities asynchronously with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .getMatching(2);
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
     */
    public AchillesFuture<List<TYPE>> getMatching(Object... matchedClusteringKeys) {
        super.withClusteringsInternal(matchedClusteringKeys);
        return super.asyncGetInternal();
    }

    /**
     *
     * Get first entity asynchronously with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .getOneMatching(2);
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2</strong> ORDER BY rating ASC <strong>LIMIT 1</strong>
     *
     * @return AchillesFuture&lt;TYPE&gt;
     */
    public AchillesFuture<TYPE> getOneMatching(Object... matchedClusteringKeys) {
        super.withClusteringsInternal(matchedClusteringKeys);
        return super.asyncGetOneInternal();
    }

    /**
     *
     * Get first entities asynchronously with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .getFirstMatching(10,2);
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2</strong> ORDER BY rating ASC <strong>LIMIT 10</strong>
     *
     * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
     */
    public AchillesFuture<List<TYPE>> getFirstMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(ASCENDING);
        super.properties.limit(limit);
        super.withClusteringsInternal(matchingClusteringKeys);
        return super.asyncGetInternal();
    }

    /**
     *
     * Get last entities asynchronously with matching clustering keys. It is similar to calling
     * getFirstMatching(...) combined with orderByDescending()
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .getLastMatching(10,2);
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2 ORDER BY rating DESC LIMIT 10</strong>
     *
     * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
     */
    public AchillesFuture<List<TYPE>> getLastMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(DESCENDING);
        super.withClusteringsInternal(matchingClusteringKeys);
        super.properties.limit(limit);
        return super.asyncGetInternal();
    }

    /**
     *
     * Filter with lower bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .fromClusterings(2,now)
     *      .get();
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND (rating,date)&gt;=(2,now)</strong> ORDER BY rating ASC LIMIT 100
     * <br/>
     * <br/>
     * <em>Remark: the generated CQL  query will take into account the defined clustering order of the table. For example if the clustering order is <strong>descending</strong>,</em>
     * <pre class="code"><code class="java">
     *     fromClustering("col1","col2)
     * </code></pre>
     * <em>will generate</em>
     * <pre class="code"><code class="sql">
     *     WHERE (col1,col2)<=(:col1,:col2)
     * </code></pre>
     *
     * @return slice DSL
     */
    public SelectFromClusteringsAsync<TYPE> fromClusterings(Object... fromClusteringKeys) {
        super.fromClusteringsInternal(fromClusteringKeys);
        return new SelectFromClusteringsAsync<>();
    }

    /**
     *
     * Filter with upper bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .toClusterings(3)
     *      .get();
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating&lt;=3</strong> ORDER BY rating ASC LIMIT 100
     * <br/>
     * <br/>
     * <em>Remark: the generated CQL  query will take into account the defined clustering order of the table. For example if the clustering order is <strong>descending</strong>,</em>
     * <pre class="code"><code class="java">
     *     toClustering("col1","col2)
     * </code></pre>
     * <em>will generate</em>
     * <pre class="code"><code class="sql">
     *     WHERE (col1,col2)>=(:col1,:col2)
     * </code></pre>
     *
     * @return slice DSL
     */
    public SelectEndAsync<TYPE> toClusterings(Object... toClusteringKeys) {
        super.toClusteringsInternal(toClusteringKeys);
        return new SelectEndAsync<>();
    }

    /**
     *
     * Filter with matching clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forSelect()
     *      .withPartitionComponents(articleId)
     *      .withClusterings(3)
     *      .get();
     *
     * </code></pre>
     *
     * Generated CQL  query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=3</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public SelectWithClusteringsAsync<TYPE> withClusterings(Object... clusteringKeys) {
        super.withClusteringsInternal(clusteringKeys);
        return new SelectWithClusteringsAsync<>();
    }

    public abstract class SelectClusteringsRootWithLimitationAsync<ENTITY_TYPE, T extends SelectClusteringsRootWithLimitationAsync<ENTITY_TYPE, T>> {
        /**
         *
         * Use ascending order for the first clustering key. This is the <strong>default</strong> ordering
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .orderByAscending()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating ASC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByAscending() {
            AsyncSelectPartitionRoot.super.properties.ordering(ASCENDING);
            return getThis();
        }

        /**
         *
         * Use descending order for the first clustering key
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .orderByDescending()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating DESC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByDescending() {
            AsyncSelectPartitionRoot.super.properties.ordering(DESCENDING);
            return getThis();
        }

        /**
         *
         * Set a limit to the query. <strong>A default limit of 100 is always set to avoid OutOfMemoryException</strong>
         * You can remove it at your own risk using noLimit()
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .limit(5)
         *      .get();
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 ORDER BY rating ASC <strong>LIMIT 5</strong>
         * @return Slice DSL
         */
        public T limit(int limit) {
            AsyncSelectPartitionRoot.super.properties.limit(limit);
            return getThis();
        }
		
        /**
         *
         * Provide a consistency level for SELECT statement
         *
         * @param consistencyLevel
         * @return Slice DSL
         */
        public T withConsistency(ConsistencyLevel consistencyLevel) {
            AsyncSelectPartitionRoot.super.properties.consistency(consistencyLevel);
            return getThis();
        }

        public T withAsyncListeners(FutureCallback<Object>...asyncListeners) {
            AsyncSelectPartitionRoot.this.properties.asyncListeners(asyncListeners);
            return getThis();
        }

        protected abstract T getThis();

        /**
         *
         * Get first entity asynchronously with filtering clustering keys
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2)
         *      .getOne();
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating&gt;=2 ORDER BY rating ASC <strong>LIMIT 1</strong>
         *
         * @return AchillesFuture&lt;TYPE&gt;
         */
        public AchillesFuture<TYPE> getOne() {
            AsyncSelectPartitionRoot.super.properties.limit(1);
            return AsyncSelectPartitionRoot.super.asyncGetOneInternal();
        }

        /**
         *
         * Get first entity asynchronously with filtering clustering keys
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2)
         *      .get();
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating&gt;=2 ORDER BY rating ASC LIMIT 100
         *
         * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
         */
        public AchillesFuture<List<TYPE>> get() {
            return AsyncSelectPartitionRoot.super.asyncGetInternal();
        }

        /**
         *
         * Get first entity asynchronously by filtering clustering keys with provided limit
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2)
         *      .get(23);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating&gt;=2 ORDER BY rating ASC <strong>LIMIT 23</strong>
         *
         * @return AchillesFuture&lt;List&lt;TYPE&gt;&gt;
         */
        public AchillesFuture<List<TYPE>> get(int limit) {
            AsyncSelectPartitionRoot.super.properties.limit(limit);
            return AsyncSelectPartitionRoot.super.asyncGetInternal();
        }
    }

    public abstract class SelectClusteringsRootAsync<ENTITY_TYPE, T extends SelectClusteringsRootAsync<ENTITY_TYPE, T>> extends SelectClusteringsRootWithLimitationAsync<ENTITY_TYPE, T> {

        /**
         *
         * Use inclusive upper & lower bounds
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withInclusiveBounds()
         *      .get(20);
         *
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withInclusiveBounds() {
            AsyncSelectPartitionRoot.super.properties.bounding(INCLUSIVE_BOUNDS);
            return getThis();
        }

        /**
         *
         * Use exclusive upper & lower bounds
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withExclusiveBounds() {
            AsyncSelectPartitionRoot.super.properties.bounding(EXCLUSIVE_BOUNDS);
            return getThis();
        }

        /**
         *
         * Use inclusive lower bound and exclusive upper bounds
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromInclusiveToExclusiveBounds() {
            AsyncSelectPartitionRoot.super.properties.bounding(INCLUSIVE_START_BOUND_ONLY);
            return getThis();
        }

        /**
         *
         * Use exclusive lower bound and inclusive upper bounds
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromExclusiveToInclusiveBounds() {
            AsyncSelectPartitionRoot.super.properties.bounding(INCLUSIVE_END_BOUND_ONLY);
            return getThis();
        }


    }

    public class SelectFromClusteringsAsync<ENTITY_TYPE> extends SelectClusteringsRootAsync<ENTITY_TYPE, SelectFromClusteringsAsync<ENTITY_TYPE>> {

        /**
         *
         * Filter with upper bound clustering key(s)
         *
         * <pre class="code"><code class="java">
         *
         *  asyncManager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .toClusterings(3)
         *      .get();
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating&lt;=3</strong> ORDER BY rating ASC LIMIT 100
         * <br/>
         * <br/>
         * <em>Remark: the generated CQL  query will take into account the defined clustering order of the table. For example if the clustering order is <strong>descending</strong>,</em>
         * <pre class="code"><code class="java">
         *     toClustering("col1","col2)
         * </code></pre>
         * <em>will generate</em>
         * <pre class="code"><code class="sql">
         *     WHERE (col1,col2)>=(:col1,:col2)
         * </code></pre>
         *
         * @return slice DSL
         */
        public SelectEndAsync<ENTITY_TYPE> toClusterings(Object... clusteringKeys) {
            AsyncSelectPartitionRoot.super.toClusteringsInternal(clusteringKeys);
            return new SelectEndAsync<>();
        }

        @Override
        protected SelectFromClusteringsAsync<ENTITY_TYPE> getThis() {
            return SelectFromClusteringsAsync.this;
        }
    }

    public class SelectWithClusteringsAsync<ENTITY_TYPE> extends SelectClusteringsRootWithLimitationAsync<ENTITY_TYPE, SelectWithClusteringsAsync<ENTITY_TYPE>> {

        /**
         *
         * Filter with clustering key(s) IN a list of provided values
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .withClusterings(3)
         *      .andClusteringsIN(now,tomorrow,yesterday)
         *      .get();
         *
         * </code></pre>
         *
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating=3 AND <strong>date IN (now,tomorrow,yesterday)</strong> ORDER BY rating ASC LIMIT 100
         *
         * @return slice DSL
         */
        public SelectEndWithLimitationAsync<ENTITY_TYPE> andClusteringsIN(Object... clusteringKeys) {
            AsyncSelectPartitionRoot.super.andClusteringsInInternal(clusteringKeys);
            return new SelectEndWithLimitationAsync<>();
        }

        @Override
        protected SelectWithClusteringsAsync<ENTITY_TYPE> getThis() {
            return SelectWithClusteringsAsync.this;
        }
    }

    public class SelectEndAsync<ENTITY_TYPE> extends SelectClusteringsRootAsync<ENTITY_TYPE, SelectEndAsync<ENTITY_TYPE>> {

        @Override
        protected SelectEndAsync<ENTITY_TYPE> getThis() {
            return SelectEndAsync.this;
        }
    }

    public class SelectEndWithLimitationAsync<ENTITY_TYPE> extends SelectClusteringsRootWithLimitationAsync<ENTITY_TYPE, SelectEndWithLimitationAsync<ENTITY_TYPE>> {

        @Override
        protected SelectEndWithLimitationAsync<ENTITY_TYPE> getThis() {
            return SelectEndWithLimitationAsync.this;
        }
    }

//    public class SelectPartitionAsyncRoot {
//
//        public AchillesFuture<List<TYPE>> get() {
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> get(int limit) {
//            AsyncSelectPartitionRoot.super.properties.limit(limit);
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//
//        public AchillesFuture<TYPE> getOne() {
//            AsyncSelectPartitionRoot.super.properties.limit(1);
//            return AsyncSelectPartitionRoot.super.asyncGetOneInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> getMatching(Object... matchedClusteringKeys) {
//            AsyncSelectPartitionRoot.super.withClusteringsInternal(matchedClusteringKeys);
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//
//        public AchillesFuture<TYPE> getOneMatching(Object... matchedClusteringKeys) {
//            AsyncSelectPartitionRoot.super.withClusteringsInternal(matchedClusteringKeys);
//            return AsyncSelectPartitionRoot.super.asyncGetOneInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> getFirstMatching(int limit, Object... matchingClusteringKeys) {
//            AsyncSelectPartitionRoot.super.properties.ordering(ASCENDING);
//            AsyncSelectPartitionRoot.super.properties.limit(limit);
//            AsyncSelectPartitionRoot.super.withClusteringsInternal(matchingClusteringKeys);
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> getLastMatching(int limit, Object... matchingClusteringKeys) {
//            AsyncSelectPartitionRoot.super.properties.ordering(DESCENDING);
//            AsyncSelectPartitionRoot.super.withClusteringsInternal(matchingClusteringKeys);
//            AsyncSelectPartitionRoot.super.properties.limit(limit);
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//    }
//
//    public class SelectClusteringRootAsync {
//
//        public AchillesFuture<TYPE> getOne() {
//            AsyncSelectPartitionRoot.super.properties.limit(1);
//            return AsyncSelectPartitionRoot.super.asyncGetOneInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> get() {
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//
//        public AchillesFuture<List<TYPE>> get(int limit) {
//            AsyncSelectPartitionRoot.super.properties.limit(limit);
//            return AsyncSelectPartitionRoot.super.asyncGetInternal();
//        }
//    }
}
