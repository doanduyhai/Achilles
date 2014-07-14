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

import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import java.util.Iterator;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class IteratePartitionRoot<TYPE, T extends IteratePartitionRoot<TYPE,T>> extends SliceQueryRootExtended<TYPE, T> {

    protected IteratePartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceQueryProperties.SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }

    public Iterator<TYPE> iterator() {
        return super.iteratorInternal();
    }

    public Iterator<TYPE> iterator(int batchSize) {
        super.properties.batchSize(batchSize);
        return super.iteratorInternal();
    }

    public Iterator<TYPE> iteratorWithMatching(Object... clusterings) {
        super.withClusteringsInternal(clusterings);
        return super.iteratorInternal();
    }

    public Iterator<TYPE> iteratorWithMatchingAndBatchSize(int batchSize, Object... clusterings) {
        super.properties.batchSize(batchSize);
        super.withClusteringsInternal(clusterings);
        return super.iteratorInternal();
    }

    public IterateFromClusterings<TYPE> fromClusterings(Object... clusteringKeys) {
        super.fromClusteringsInternal(clusteringKeys);
        return new IterateFromClusterings<>();
    }

    public IterateEnd<TYPE> toClusterings(Object... clusteringKeys) {
        super.toClusteringsInternal(clusteringKeys);
        return new IterateEnd<>();
    }

    public IterateWithClusterings<TYPE> withClusterings(Object... clusteringKeys) {
        super.withClusteringsInternal(clusteringKeys);
        return new IterateWithClusterings<>();
    }

    public abstract class IterateClusteringsRootWithLimitation<ENTITY_TYPE, T extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, T>> {
        public T orderByAscending() {
            IteratePartitionRoot.super.properties.ordering(ASCENDING);
            return getThis();
        }

        public T orderByDescending() {
            IteratePartitionRoot.super.properties.ordering(DESCENDING);
            return getThis();
        }


        public T limit(int limit) {
            IteratePartitionRoot.super.properties.limit(limit);
            return getThis();
        }


        public T withConsistency(ConsistencyLevel consistencyLevel) {
            IteratePartitionRoot.super.properties.consistency(consistencyLevel);
            return getThis();
        }

        protected abstract T getThis();

        public Iterator<TYPE> iterator() {
            return IteratePartitionRoot.super.iteratorInternal();
        }

        public Iterator<TYPE> iterator(int batchSize) {
            IteratePartitionRoot.super.properties.batchSize(batchSize);
            return IteratePartitionRoot.super.iteratorInternal();
        }
    }

    public abstract class IterateClusteringsRoot<ENTITY_TYPE, T extends IterateClusteringsRoot<ENTITY_TYPE, T>> extends IterateClusteringsRootWithLimitation<ENTITY_TYPE, T> {

        public T withInclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_BOUNDS);
            return getThis();
        }


        public T withExclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(EXCLUSIVE_BOUNDS);
            return getThis();
        }


        public T fromInclusiveToExclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_START_BOUND_ONLY);
            return getThis();
        }


        public T fromExclusiveToInclusiveBounds() {
            IteratePartitionRoot.super.properties.bounding(INCLUSIVE_END_BOUND_ONLY);
            return getThis();
        }
    }

    public class IterateFromClusterings<ENTITY_TYPE> extends IterateClusteringsRoot<ENTITY_TYPE, IterateFromClusterings<ENTITY_TYPE>> {

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
}
