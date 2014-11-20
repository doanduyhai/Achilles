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

/**
 * Builder for slice query
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#slice-query" target="_blank">Slice Query DSL</a>
 *
 * @param <TYPE>: type of clustered entity
 */
public class SliceQueryBuilderAsync<TYPE> {

    private final SliceQueryExecutor sliceQueryExecutor;
    private final Class<TYPE> entityClass;
    private final EntityMeta meta;

    public SliceQueryBuilderAsync(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
    }

    /**
     * Create a builder DSL for a SELECT statement
     *
     * @return SelectDSLAsync
     */
    public SelectDSLAsync<TYPE> forSelect() {
        return new SelectDSLAsync<>(sliceQueryExecutor, entityClass, meta, SliceType.SELECT);
    }

    /**
     * Create a builder DSL for iteration on a SELECT statement
     *
     * @return IterateDSLAsync
     */
    public IterateDSLAsync<TYPE> forIteration() {
        return new IterateDSLAsync<>(sliceQueryExecutor, entityClass, meta, SliceType.ITERATE);
    }

    /**
     * Create a builder DSL for a DELETE statement
     *
     * @return DeleteDSLAsync
     */
    public DeleteDSLAsync<TYPE> forDelete() {
        return new DeleteDSLAsync<>(sliceQueryExecutor, entityClass, meta, SliceType.DELETE);
    }

}
