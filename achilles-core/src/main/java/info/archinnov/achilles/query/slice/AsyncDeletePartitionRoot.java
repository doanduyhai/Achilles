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

import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Empty;

public abstract class AsyncDeletePartitionRoot<TYPE, T extends AsyncDeletePartitionRoot<TYPE, T>> extends SliceQueryRoot<TYPE, T> {

    protected AsyncDeletePartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceQueryProperties.SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    /**
     *
     * Delete entities asynchronously without filtering clustering keys.
     * The return future contains an Empty singleton which only indicates
     * that the deletion has been successful
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forDelete()
     *      .withPartitionComponents(articleId)
     *      .delete();
     *
     * </code></pre>
     *
     * Generated CQL query:
     *
     * <br/>
     *  DELETE FROM article_rating WHERE article_id=...
     *
     * @return AchillesFuture&lt;Empty&gt;
     */
    public AchillesFuture<Empty> delete() {
        return super.asyncDeleteInternal();
    }

    /**
     *
     * Delete entities asynchronously with matching clustering keys
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forDelete()
     *      .withPartitionComponents(articleId)
     *      .deleteMatching(2);
     *
     * </code></pre>
     *
     * Generated CQL query:
     *
     * <br/>
     *  DELETE FROM article_rating WHERE article_id=... <strong>AND rating=2</strong>
     *
     * @return AchillesFuture&lt;Empty&gt;
     */
    public AchillesFuture<Empty> deleteMatching(Object... clusterings) {
        super.withClusteringsInternal(clusterings);
        return super.asyncDeleteInternal();
    }

    /**
     *
     * Provide a consistency level for DELETE statement
     *
     * @param consistencyLevel
     * @return Slice DSL
     */
    public T withConsistency(ConsistencyLevel consistencyLevel) {
        this.properties.writeConsistency(consistencyLevel);
        return getThis();
    }
}
