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

/**
 * Builder for slice query
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#slice-query" target="_blank">Slice Query DSL</a>
 *
 * @param <TYPE>: type of clustered entity
 */
public class SliceQueryBuilder<TYPE> {

    private final SliceQueryExecutor sliceQueryExecutor;
    private final Class<TYPE> entityClass;
    private final EntityMeta meta;

    public SliceQueryBuilder(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
    }

    /**
     * Create a builder DSL for a SELECT statement
     *
     * @return SelectDSL
     */
    public SelectDSL<TYPE> forSelect() {
        return new SelectDSL<>(sliceQueryExecutor, entityClass, meta, SliceType.SELECT);
    }

    /**
     * Create a builder DSL for iteration on a SELECT statement
     *
     * @return IterateDSL
     */
    public IterateDSL<TYPE> forIteration() {
        return new IterateDSL<>(sliceQueryExecutor, entityClass, meta, SliceType.ITERATE);
    }

    /**
     * Create a builder DSL for a DELETE statement
     *
     * @return DeleteDSL
     */
    public DeleteDSL<TYPE> forDelete() {
        return new DeleteDSL<>(sliceQueryExecutor, entityClass, meta, SliceType.DELETE);
    }

}
