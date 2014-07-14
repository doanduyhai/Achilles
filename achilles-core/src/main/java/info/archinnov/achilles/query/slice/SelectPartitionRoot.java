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
import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import java.util.List;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class SelectPartitionRoot<TYPE, T extends SelectPartitionRoot<TYPE,T>> extends SliceQueryRootExtended<TYPE, T> {

    protected SelectPartitionRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        super(sliceQueryExecutor, entityClass, meta, sliceType);
    }


    public List<TYPE> get() {
        return super.getInternal();
    }

    public List<TYPE> get(int limit) {
        super.properties.limit(limit);
        return super.getInternal();
    }

    public TYPE getOne() {
        super.properties.limit(1);
        return FluentIterable.from(super.getInternal()).first().orNull();
    }

    public List<TYPE> getMatching(Object... matchedClusteringKeys) {
        super.withClusteringsInternal(matchedClusteringKeys);
        return super.getInternal();
    }

    public TYPE getOneMatching(Object... matchedClusteringKeys) {
        return FluentIterable.from(this.getMatching(matchedClusteringKeys)).first().orNull();
    }

    public List<TYPE> getFirstMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(ASCENDING);
        super.properties.limit(limit);
        super.withClusteringsInternal(matchingClusteringKeys);
        return super.getInternal();
    }

    public List<TYPE> getLastMatching(int limit, Object... matchingClusteringKeys) {
        super.properties.ordering(DESCENDING);
        super.withClusteringsInternal(matchingClusteringKeys);
        super.properties.limit(limit);
        return super.getInternal();
    }

    public SelectFromClusterings<TYPE> fromClusterings(Object... fromClusteringKeys) {
        super.fromClusteringsInternal(fromClusteringKeys);
        return new SelectFromClusterings<>();
    }

    public SelectEnd<TYPE> toClusterings(Object... toClusteringKeys) {
        super.toClusteringsInternal(toClusteringKeys);
        return new SelectEnd<>();
    }

    public SelectWithClusterings<TYPE> withClusterings(Object... clusteringKeys) {
        super.withClusteringsInternal(clusteringKeys);
        return new SelectWithClusterings<>();
    }

    public abstract class SelectClusteringsRootWithLimitation<ENTITY_TYPE, T extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, T>> {

        public T orderByAscending() {
            SelectPartitionRoot.super.properties.ordering(ASCENDING);
            return getThis();
        }

        public T orderByDescending() {
            SelectPartitionRoot.super.properties.ordering(DESCENDING);
            return getThis();
        }

        public T limit(int limit) {
            SelectPartitionRoot.super.properties.limit(limit);
            return getThis();
        }

        public T withConsistency(ConsistencyLevel consistencyLevel) {
            SelectPartitionRoot.super.properties.consistency(consistencyLevel);
            return getThis();
        }

        protected abstract T getThis();

        public TYPE getOne() {
            SelectPartitionRoot.super.properties.limit(1);
            return FluentIterable.from(SelectPartitionRoot.super.getInternal()).first().orNull();
        }

        public List<TYPE> get() {
            return SelectPartitionRoot.super.getInternal();
        }

        public List<TYPE> get(int limit) {
            SelectPartitionRoot.super.properties.limit(limit);
            return SelectPartitionRoot.super.getInternal();
        }
    }

    public abstract class SelectClusteringsRoot<ENTITY_TYPE, T extends SelectClusteringsRoot<ENTITY_TYPE, T>> extends SelectClusteringsRootWithLimitation<ENTITY_TYPE, T> {

        public T withInclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_BOUNDS);
            return getThis();
        }

        public T withExclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(EXCLUSIVE_BOUNDS);
            return getThis();
        }

        public T fromInclusiveToExclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_START_BOUND_ONLY);
            return getThis();
        }

        public T fromExclusiveToInclusiveBounds() {
            SelectPartitionRoot.super.properties.bounding(INCLUSIVE_END_BOUND_ONLY);
            return getThis();
        }


    }

    public class SelectFromClusterings<ENTITY_TYPE> extends SelectClusteringsRoot<ENTITY_TYPE, SelectFromClusterings<ENTITY_TYPE>> {

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
