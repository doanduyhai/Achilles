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
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;

public abstract class DeletePartitionRoot<TYPE, T extends DeletePartitionRoot<TYPE, T>> extends SliceQueryRoot<TYPE, T> {

    protected DeletePartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceQueryProperties.SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    /**
     *
     * Delete entities without filtering clustering keys.
     *
     * <pre class="code"><code class="java">
     *
     *  manager.sliceQuery(ArticleRating.class)
     *      .forDelete()
     *      .withPartitionComponents(articleId)
     *      .delete();
     *
     * </code></pre>
     *
     * Generated CQL3 query:
     *
     * <br/>
     *  DELETE FROM article_rating WHERE article_id=...
     *
     * @return slice DSL
     */
    public void delete() {
        super.properties.disableLimit();
        super.deleteInternal();
    }

    /**
     *
     * Delete entities with matching clustering keys
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
     * Generated CQL3 query:
     *
     * <br/>
     *  DELETE FROM article_rating WHERE article_id=... <strong>AND rating=2</strong>
     *
     * @return slice DSL
     */
    public void deleteMatching(Object... clusterings) {
        super.properties.disableLimit();
        super.withClusteringsInternal(clusterings);
        super.deleteInternal();
    }

    public  DeletePartitionRootAsync async() {
        return new DeletePartitionRootAsync();
    }

    public class DeletePartitionRootAsync {

        public AchillesFuture<Empty> delete() {
            DeletePartitionRoot.super.properties.disableLimit();
            return DeletePartitionRoot.super.asyncDeleteInternal();
        }


        public AchillesFuture<Empty> deleteMatching(Object... clusterings) {
            DeletePartitionRoot.super.properties.disableLimit();
            DeletePartitionRoot.super.withClusteringsInternal(clusterings);
            return DeletePartitionRoot.super.asyncDeleteInternal();
        }

    }
}
