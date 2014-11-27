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
import static info.archinnov.achilles.query.slice.SliceQueryProperties.SliceType;

import java.util.List;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class SelectPartitionRoot<TYPE, T extends SelectPartitionRoot<TYPE,T>> extends SliceQueryRootExtended<TYPE, T> {

    protected SelectPartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    /**
     *
     * Get selected entities without filtering clustering keys. If no limit has been set, the default LIMIT 100 applies
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public List<TYPE> get() {
        return super.getInternal();
    }

    /**
     *
     * Get selected entities without filtering clustering keys using provided limit
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public List<TYPE> get(int limit) {
        super.properties.limit(limit);
        return super.getInternal();
    }

    /**
     *
     * Get first entity without filtering clustering keys
     * To get the last entity, just use getOne() with orderByDescending()
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public TYPE getOne() {
        super.properties.limit(1);
        return FluentIterable.from(super.getInternal()).first().orNull();
    }

    /**
     *
     * Get entities with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public List<TYPE> getMatching(Object... matchedClusteringKeys) {
        super.withClusteringsInternal(matchedClusteringKeys);
        return super.getInternal();
    }

    /**
     *
     * Get first entity with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public TYPE getOneMatching(Object... matchedClusteringKeys) {
        return FluentIterable.from(this.getMatching(matchedClusteringKeys)).first().orNull();
    }

    /**
     *
     * Get first entities with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public List<TYPE> getFirstMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(ASCENDING);
        super.properties.limit(limit);
        super.withClusteringsInternal(matchingClusteringKeys);
        return super.getInternal();
    }

    /**
     *
     * Get last entities with matching clustering keys. It is similar to calling
     * getFirstMatching(...) combined with orderByDescending()
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
     * @return slice DSL
     */
    public List<TYPE> getLastMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(DESCENDING);
        super.withClusteringsInternal(matchingClusteringKeys);
        super.properties.limit(limit);
        return super.getInternal();
    }

    /**
     *
     * Filter with lower bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
    public SelectFromClusterings<TYPE> fromClusterings(Object... fromClusteringKeys) {
        super.fromClusteringsInternal(fromClusteringKeys);
        return new SelectFromClusterings<>();
    }

    /**
     *
     * Filter with upper bound clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
    public SelectEnd<TYPE> toClusterings(Object... toClusteringKeys) {
        super.toClusteringsInternal(toClusteringKeys);
        return new SelectEnd<>();
    }

    /**
     *
     * Filter with matching clustering key(s)
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
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
    public SelectWithClusterings<TYPE> withClusterings(Object... clusteringKeys) {
        super.withClusteringsInternal(clusteringKeys);
        return new SelectWithClusterings<>();
    }

    public abstract class SelectClusteringsRootWithLimitation<ENTITY_TYPE, T extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, T>> {


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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating ASC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByAscending() {
            SelectPartitionRoot.super.properties.ordering(ASCENDING);
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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 <strong>ORDER BY rating DESC</strong> LIMIT 20
         * @return Slice DSL
         */
        public T orderByDescending() {
            SelectPartitionRoot.super.properties.ordering(DESCENDING);
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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND (rating,date)&gt;=(2,now) AND (rating)&lt;=4 ORDER BY rating ASC <strong>LIMIT 5</strong>
         * @return Slice DSL
         */
        public T limit(int limit) {
            SelectPartitionRoot.super.properties.limit(limit);
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
            SelectPartitionRoot.super.properties.readConsistency(consistencyLevel);
            return getThis();
        }

        public T withAsyncListeners(FutureCallback<Object>...asyncListeners) {
            SelectPartitionRoot.this.properties.asyncListeners(asyncListeners);
            return getThis();
        }

        protected abstract T getThis();

        /**
         *
         * Get first entity with filtering clustering keys
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
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
         * @return slice DSL
         */
        public TYPE getOne() {
            SelectPartitionRoot.super.properties.limit(1);
            return FluentIterable.from(SelectPartitionRoot.super.getInternal()).first().orNull();
        }

        /**
         *
         * Get first entity with filtering clustering keys
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
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
         * @return slice DSL
         */
        public List<TYPE> get() {
            return SelectPartitionRoot.super.getInternal();
        }

        /**
         *
         * Get first entity with filtering clustering keys with provided limit
         * To get the last entity, just use getOne() with orderByDescending()
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
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
         * @return slice DSL
         */
        public List<TYPE> get(int limit) {
            SelectPartitionRoot.super.properties.limit(limit);
            return SelectPartitionRoot.super.getInternal();
        }
    }

    public abstract class SelectClusteringsRoot<ENTITY_TYPE, T extends SelectClusteringsRoot<ENTITY_TYPE, T>> extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, T> {

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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withInclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_BOUNDS);
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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T withExclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(EXCLUSIVE_BOUNDS);
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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;=(2,now)</strong> AND <strong>(rating)&lt;4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromInclusiveToExclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_START_BOUND_ONLY);
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
         * Generated CQL  query:
         *
         * <br/>
         *  SELECT * FROM article_rating WHERE article_id=... AND <strong>(rating,date)&gt;(2,now)</strong> AND <strong>(rating)&lt;=4</strong> ORDER BY rating ASC LIMIT 20
         * @return Slice DSL
         */
        public T fromExclusiveToInclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_END_BOUND_ONLY);
            return getThis();
        }


    }

    public class SelectFromClusterings<ENTITY_TYPE> extends SelectClusteringsRoot<ENTITY_TYPE, SelectFromClusterings<ENTITY_TYPE>> {

        /**
         *
         * Filter with upper bound clustering key(s)
         *
         * <pre class="code"><code class="java">
         *
         *  manager.sliceQuery(ArticleRating.class)
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
        public SelectEnd<ENTITY_TYPE> toClusterings(Object... clusteringKeys) {
            SelectPartitionRoot.super.toClusteringsInternal(clusteringKeys);
            return new SelectEnd<>();
        }

        @Override
        protected SelectFromClusterings<ENTITY_TYPE> getThis() {
            return SelectFromClusterings.this;
        }
    }

    public class SelectWithClusterings<ENTITY_TYPE> extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, SelectWithClusterings<ENTITY_TYPE>> {

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
        public SelectEndWithLimitation<ENTITY_TYPE> andClusteringsIN(Object... clusteringKeys) {
            SelectPartitionRoot.super.andClusteringsInInternal(clusteringKeys);
            return new SelectEndWithLimitation<>();
        }

        @Override
        protected SelectWithClusterings<ENTITY_TYPE> getThis() {
            return SelectWithClusterings.this;
        }
    }

    public class SelectEnd<ENTITY_TYPE> extends SelectClusteringsRoot<ENTITY_TYPE, SelectEnd<ENTITY_TYPE>> {

        @Override
        protected SelectEnd<ENTITY_TYPE> getThis() {
            return SelectEnd.this;
        }
    }

    public class SelectEndWithLimitation<ENTITY_TYPE> extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, SelectEndWithLimitation<ENTITY_TYPE>> {

        @Override
        protected SelectEndWithLimitation<ENTITY_TYPE> getThis() {
            return SelectEndWithLimitation.this;
        }
    }
}
