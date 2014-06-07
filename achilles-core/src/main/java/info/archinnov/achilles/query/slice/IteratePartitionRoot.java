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

import static info.archinnov.achilles.query.slice.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.query.slice.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.query.slice.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.query.slice.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.query.slice.OrderingMode.ASCENDING;
import static info.archinnov.achilles.query.slice.OrderingMode.DESCENDING;
import java.util.Iterator;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class IteratePartitionRoot<TYPE, T extends IteratePartitionRoot<TYPE,T>> extends SliceQueryRootExtended<TYPE, T> {

    protected IteratePartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceQueryProperties.SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    /**
     *
     * Iterate over selected entities without filtering clustering keys. If no limit has been set, the default LIMIT 100 applies
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .iterator();
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public Iterator<TYPE> iterator() {
        return super.iteratorInternal();
    }

    /**
     *
     * Iterate over selected entities without filtering clustering keys using provided fetch size
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .iterator(23);
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC ASC LIMIT 100
     *
     * <br/>
     * <em><strong>
     * Note: if the fetch size is set, then you won't be able to use IN clause for partition components together ORDER BY.
     * In this case, Achilles will remove automatically the ORDER BY clause
     * </em></strong>
     *
     * @return slice DSL
     */
    public Iterator<TYPE> iterator(int batchSize) {
        super.properties.fetchSize(batchSize);
        return super.iteratorInternal();
    }

    /**
     *
     * Iterate over entities with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .iteratorWithMatching(2);
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public Iterator<TYPE> iteratorWithMatching(Object... clusterings) {
        super.withClusteringsInternal(clusterings);
        return super.iteratorInternal();
    }

    /**
     *
     * Iterate over entities with matching clustering keys and fetch size
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .iteratorWithMatchingAndBatchSize(10,2);
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=2</strong> ORDER BY rating ASC LIMIT 100
     *
     * <em><strong>
     * Note: if the fetch size is set, then you won't be able to use IN clause for partition components together ORDER BY.
     * In this case, Achilles will remove automatically the ORDER BY clause
     * </em></strong>
     *
     * @return slice DSL
     */
    public Iterator<TYPE> iteratorWithMatchingAndBatchSize(int batchSize, Object... clusterings) {
        super.properties.fetchSize(batchSize);
        super.withClusteringsInternal(clusterings);
        return super.iteratorInternal();
    }


    public IteratePartitionRootAsync async() {
        return new IteratePartitionRootAsync();
    }

    /**
     *
     * Filter with lower bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .fromClusterings(2,now)
     *      .iterator();
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND (rating,date)&gt;=(2,now)</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public IterateFromClusterings<TYPE> fromClusterings(Object... clusteringKeys) {
        super.fromClusteringsInternal(clusteringKeys);
        return new IterateFromClusterings<>();
    }

    /**
     *
     * Filter with upper bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .toClusterings(3)
     *      .iterator();
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating&lt;=3</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public IterateEnd<TYPE> toClusterings(Object... clusteringKeys) {
        super.toClusteringsInternal(clusteringKeys);
        return new IterateEnd<>();
    }

    /**
     *
     * Filter with matching clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forIterate()
     *      .withPartitionComponents(articleId)
     *      .withClusterings(3)
     *      .iterator();
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating=3</strong> ORDER BY rating ASC LIMIT 100
     *
     * @return slice DSL
     */
    public IterateWithClusterings<TYPE> withClusterings(Object... clusteringKeys) {
        super.withClusteringsInternal(clusteringKeys);
        return new IterateWithClusterings<>();
    }

    public abstract class IterateClusteringsRootWithLimitation<ENTITY_TYPE, T extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, T>> {

        /**
         *
         * Use ascending order for the first clustering key. This is the <strong>default</strong> ordering
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .orderByAscending()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating ASC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByAscending() {
            IteratePartitionRoot.super.properties.ordering(ASCENDING);
            return getThis();
        }

        /**
         *
         * Use descending order for the first clustering key
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .orderByDescending()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating DESC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByDescending() {
            IteratePartitionRoot.super.properties.ordering(DESCENDING);
            return getThis();
        }

        /**
         *
         * Set a limit to the query. <strong>A default limit of 100 is always set to avoid OutOfMemoryException</strong>
         * You can remove it at your own risk using noLimit()
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .limit(5)
         *      .get();
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 ORDER BY rating ASC <strong>LIMIT 5</strong>
         * @return Slice DSL
         */
        public T limit(int limit) {
            IteratePartitionRoot.super.properties.limit(limit);
            return getThis();
        }

        /**
         * Remove the default limit of 100 rows
         *
         * @return Slice DSL
         */
        public T noLimit() {
            IteratePartitionRoot.super.properties.disableLimit();
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
            IteratePartitionRoot.super.properties.consistency(consistencyLevel);
            return getThis();
        }

        public T withAsyncListeners(FutureCallback<Object>...asyncListeners) {
            IteratePartitionRoot.this.properties.asyncListeners(asyncListeners);
            return getThis();
        }

        protected abstract T getThis();

        public IterateClusteringsRootAsync async() {
            return new IterateClusteringsRootAsync();
        }

        /**
         *
         * Iterate over entities with filtering clustering keys
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forIterate()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2)
         *      .iterator();
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating&gt;=2 ORDER BY rating ASC LIMIT 100
         *
         * @return slice DSL
         */
        public Iterator<TYPE> iterator() {
            return IteratePartitionRoot.super.iteratorInternal();
        }

        /**
         *
         * Iterate over entities with filtering clustering keys and fetch zie
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forIterate()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2)
         *      .iterator(12);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating&gt;=2 ORDER BY rating ASC LIMIT 100
         *
         * <em><strong>
         * Note: if the fetch size is set, then you won't be able to use IN clause for partition components together ORDER BY.
         * In this case, Achilles will remove automatically the ORDER BY clause
         * </em></strong>
         *
         * @return slice DSL
         */
        public Iterator<TYPE> iterator(int batchSize) {
            IteratePartitionRoot.super.properties.fetchSize(batchSize);
            return IteratePartitionRoot.super.iteratorInternal();
        }
    }

    public abstract class IterateClusteringsRoot<ENTITY_TYPE, T extends IterateClusteringsRoot<ENTITY_TYPE, T>> extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, T> {

        /**
         *
         * Use inclusive upper & lower bounds
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
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
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withInclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_BOUNDS);
            return getThis();
        }

        /**
         *
         * Use exclusive upper & lower bounds
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withExclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(EXCLUSIVE_BOUNDS);
            return getThis();
        }

        /**
         *
         * Use inclusive lower bound and exclusive upper bounds
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromInclusiveToExclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_START_BOUND_ONLY);
            return getThis();
        }

        /**
         *
         * Use exclusive lower bound and inclusive upper bounds
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forSelect()
         *      .withPartitionComponents(articleId)
         *      .fromClusterings(2, now)
         *      .toClusterings(4)
         *      .withExclusiveBounds()
         *      .get(20);
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromExclusiveToInclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_END_BOUND_ONLY);
            return getThis();
        }
    }

    public class IterateFromClusterings<ENTITY_TYPE> extends IterateClusteringsRoot<ENTITY_TYPE, IterateFromClusterings<ENTITY_TYPE>> {

        /**
         *
         * Filter with upper bound clustering key(s)
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forIterate()
         *      .withPartitionComponents(articleId)
         *      .toClusterings(3)
         *      .iterator();
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... <strong>AND rating&lt;=3</strong> ORDER BY rating ASC LIMIT 100
         *
         * @return slice DSL
         */
        public IterateEnd<TYPE> toClusterings(Object... clusteringKeys) {
            IteratePartitionRoot.super.toClusteringsInternal(clusteringKeys);
            return new IterateEnd<>();
        }

        @Override
        protected IterateFromClusterings<ENTITY_TYPE> getThis() {
            return IterateFromClusterings.this;
        }
    }

    public class IterateWithClusterings<ENTITY_TYPE> extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, IterateWithClusterings<ENTITY_TYPE>> {

        /**
         *
         * Filter with clustering key(s) IN a list of provided values
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
         *      .forIterate()
         *      .withPartitionComponents(articleId)
         *      .withClusterings(3)
         *      .andClusteringsIN(now,tomorrow,yesterday)
         *      .iterator();
         *
         * </code></pre>
         *
         * Generated CQL3 query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND rating=3 AND <strong>date IN (now,tomorrow,yesterday)</strong> ORDER BY rating ASC LIMIT 100
         *
         * @return slice DSL
         */
        public IterateEndWithLimitation<TYPE> andClusteringsIN(Object... clusteringKeys) {
            IteratePartitionRoot.super.andClusteringsInInternal(clusteringKeys);
            return new IterateEndWithLimitation<>();
        }

        @Override
        protected IterateWithClusterings<ENTITY_TYPE> getThis() {
            return IterateWithClusterings.this;
        }
    }

    public class IterateEnd<ENTITY_TYPE> extends IterateClusteringsRoot<ENTITY_TYPE, IterateEnd<ENTITY_TYPE>> {
        @Override
        protected IterateEnd<ENTITY_TYPE> getThis() {
            return IterateEnd.this;
        }
    }

    public class IterateEndWithLimitation<ENTITY_TYPE> extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, IterateEndWithLimitation<ENTITY_TYPE>> {
        @Override
        protected IterateEndWithLimitation<ENTITY_TYPE> getThis() {
            return IterateEndWithLimitation.this;
        }
    }

    public class IteratePartitionRootAsync {

        public AchillesFuture<Iterator<TYPE>> iterator() {
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }

        public AchillesFuture<Iterator<TYPE>> iterator(int batchSize) {
            IteratePartitionRoot.super.properties.fetchSize(batchSize);
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }

        public AchillesFuture<Iterator<TYPE>> iteratorWithMatching(Object... clusterings) {
            IteratePartitionRoot.super.withClusteringsInternal(clusterings);
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }

        public AchillesFuture<Iterator<TYPE>> iteratorWithMatchingAndBatchSize(int batchSize, Object... clusterings) {
            IteratePartitionRoot.super.properties.fetchSize(batchSize);
            IteratePartitionRoot.super.withClusteringsInternal(clusterings);
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }
    }

    public class IterateClusteringsRootAsync{

        public AchillesFuture<Iterator<TYPE>> iterator() {
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }

        public AchillesFuture<Iterator<TYPE>> iterator(int batchSize) {
            IteratePartitionRoot.super.properties.fetchSize(batchSize);
            return IteratePartitionRoot.super.asyncIteratorInternal();
        }
    }
}
