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

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;

import static info.archinnov.achilles.query.slice.SliceQueryProperties.SliceType;

public class IterateDSLAsync<TYPE> {

    private final SliceQueryExecutor sliceQueryExecutor;
    private final Class<TYPE> entityClass;
    private final EntityMeta meta;
    private final SliceType sliceType;


    protected IterateDSLAsync(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
        this.sliceType = sliceType;
    }

    /**
     *
     * Start the Iterate DSL with provided partition components
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(ArticleRating.class)
     *      .forIteration()
     *      .withPartitionComponents(articleId)
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM article_rating WHERE article_id=...
     *
     * @return slice DSL
     */
    public IterateFromPartitionAsync<TYPE> withPartitionComponents(Object... partitionKeyComponents) {
        IterateFromPartitionAsync<TYPE> iterateFromPartitionKey = new IterateFromPartitionAsync<>(sliceQueryExecutor, entityClass, meta, sliceType);
        iterateFromPartitionKey.withPartitionComponentsInternal(partitionKeyComponents);
        return iterateFromPartitionKey;
    }

    /**
     *
     * Start the Iterate DSL with provided partition components IN
     *
     * <pre class="code"><code class="java">
     *
     *  asyncManager.sliceQuery(MessageEntity.class)
     *      .forIteration()
     *      .withPartitionComponents(10L)
     *      .andPartitionComponentsIN(2013, 2014)
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  SELECT * FROM messages WHERE user_id=10 AND year IN (2013,2014)
     *
     * @return slice DSL
     */
    public IterateWithPartitionAsync<TYPE> withPartitionComponentsIN(Object... partitionKeyComponents) {
        IterateWithPartitionAsync<TYPE> iterateWithPartition = new IterateWithPartitionAsync<>(sliceQueryExecutor, entityClass, meta, sliceType);
        iterateWithPartition.withPartitionComponentsINInternal(partitionKeyComponents);
        return iterateWithPartition;
    }

}
