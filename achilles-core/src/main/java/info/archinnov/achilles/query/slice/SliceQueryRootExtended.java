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
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

public abstract class SliceQueryRootExtended<TYPE, T extends SliceQueryRootExtended<TYPE,T>> extends SliceQueryRoot<TYPE,T> {

    protected SliceQueryRootExtended(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    public T withInclusiveBounds() {
        super.properties.bounding(BoundingMode.INCLUSIVE_BOUNDS);
        return getThis();
    }

    public T withExclusiveBounds() {
        super.properties.bounding(BoundingMode.EXCLUSIVE_BOUNDS);
        return getThis();
    }

    public T fromInclusiveToExclusiveBounds() {
        super.properties.bounding(BoundingMode.INCLUSIVE_START_BOUND_ONLY);
        return getThis();
    }

    public T fromExclusiveToInclusiveBounds() {
        super.properties.bounding(BoundingMode.INCLUSIVE_END_BOUND_ONLY);
        return getThis();
    }

    public T orderByAscending() {
        super.properties.ordering(OrderingMode.ASCENDING);
        return getThis();
    }

    public T orderByDescending() {
        super.properties.ordering(OrderingMode.DESCENDING);
        return getThis();
    }

    public T limit(int limit) {
        super.properties.limit(limit);
        return getThis();
    }
}
