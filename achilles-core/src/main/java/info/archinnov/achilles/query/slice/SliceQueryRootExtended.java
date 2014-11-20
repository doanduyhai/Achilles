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

import static info.archinnov.achilles.query.slice.SliceQueryProperties.SliceType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;

public abstract class SliceQueryRootExtended<TYPE, T extends SliceQueryRootExtended<TYPE,T>> extends SliceQueryRoot<TYPE,T> {

    protected SliceQueryRootExtended(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

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
        super.properties.bounding(BoundingMode.INCLUSIVE_BOUNDS);
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
        super.properties.bounding(BoundingMode.EXCLUSIVE_BOUNDS);
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
        super.properties.bounding(BoundingMode.INCLUSIVE_START_BOUND_ONLY);
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
        super.properties.bounding(BoundingMode.INCLUSIVE_END_BOUND_ONLY);
        return getThis();
    }

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
        super.properties.ordering(OrderingMode.ASCENDING);
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
        super.properties.ordering(OrderingMode.DESCENDING);
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
        super.properties.limit(limit);
        return getThis();
    }
}
