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

package test.slice;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import info.archinnov.achilles.query.slice.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.query.slice.OrderingMode;

public class FromPartitionKey<TYPE> extends RootBuilder<TYPE,FromPartitionKey<TYPE>> {

    public FromPartitionKey() {
    }

    public FromPartitionKey<TYPE> withPartitionKeys(Object... partitionKeys) {
        super.partitionKeys = Arrays.asList(partitionKeys);
        return this;
    }

    public FromPartitionKey<TYPE> andPartitionKeysIn(Object... partitionKeys) {
        super.partitionKeysIn = Arrays.asList(partitionKeys);
        return this;
    }

    public List<TYPE> get() {
        return null;
    }

    public List<TYPE> get(int n) {
        return null;
    }

    public TYPE getOne() {
        return null;
    }

    public TYPE getMatching(Object... matchedClusterings) {
        return null;
    }

    public List<TYPE> getFirstMatching(int n, Object... matchedClusterings) {
        return null;
    }

    public List<TYPE> getLastMatching(int n, Object... matchedClusterings) {
        return null;
    }

    public Iterator<TYPE> iterator() {
        return null;
    }

    public Iterator<TYPE> iterator(int batchSize) {
        return null;
    }

    public Iterator<TYPE> iteratorWithMatching(Object... clusterings) {
        return null;
    }

    public Iterator<TYPE> iteratorWithMatching(int batchSize, Object... clusterings) {
        return null;
    }

    public void remove() {

    }

    public void remove(int n) {

    }

    public void removeMatching(Object... clusterings) {

    }

    public void removeFirstMatching(int n, Object... clusterings) {

    }

    public void removeLastMatching(int n, Object... clusterings) {

    }

    public FromClusterings<TYPE> fromClusterings(Object...clusterings) {
        super.fromClusterings = Arrays.asList(clusterings);
        return new FromClusterings<>();
    }

    public EndBuilder<TYPE> toClusterings(Object...clusterings) {
        super.toClusterings = Arrays.asList(clusterings);
        return new EndBuilder<>();
    }

    public WithClusterings<TYPE> withClusterings(Object...clusterings) {
        super.withClusterings = Arrays.asList(clusterings);
        return new WithClusterings<>();
    }

    public abstract class RootClustering<TYPE,T extends RootClustering<TYPE,T>> implements CommonProperties<T>{

        @Override
        public T inclusiveBounds() {
            FromPartitionKey.super.boundingMode = BoundingMode.INCLUSIVE_BOUNDS;
            return (T) this;
        }

        @Override
        public T exclusiveBounds() {
            FromPartitionKey.super.boundingMode = BoundingMode.EXCLUSIVE_BOUNDS;
            return (T) this;
        }

        @Override
        public T fromInclusiveToExclusiveBounds() {
            FromPartitionKey.super.boundingMode = BoundingMode.INCLUSIVE_START_BOUND_ONLY;
            return (T) this;
        }

        @Override
        public T fromExclusiveToInclusiveBounds() {
            FromPartitionKey.super.boundingMode = BoundingMode.INCLUSIVE_END_BOUND_ONLY;
            return (T) this;
        }

        @Override
        public T orderByAscending() {
            FromPartitionKey.super.orderingMode = OrderingMode.ASCENDING;
            return (T) this;
        }

        @Override
        public T orderByDescending() {
            FromPartitionKey.super.orderingMode = OrderingMode.DESCENDING;
            return (T) this;
        }

        @Override
        public T limit(int limit) {
            FromPartitionKey.super.limit = limit;
            return (T) this;
        }

        @Override
        public T withConsistency(ConsistencyLevel consistencyLevel) {
            FromPartitionKey.super.consistencyLevel = consistencyLevel;
            return (T) this;
        }

        public void check() {
            FromPartitionKey.super.check();
        }

        Object getOne() {
            return null;
        }

        List<TYPE> get() {
            return null;
        }

        public List<TYPE> get(int n) {
            return null;
        }

        public Iterator<TYPE> iterator() {
            return null;
        }

        public Iterator<TYPE> iterator(int n) {
            return null;
        }

        public void remove() {

        }

        public void remove(int n) {

        }
    }

    public class FromClusterings<TYPE> extends RootClustering<TYPE,FromClusterings<TYPE>> {
        public EndBuilder<TYPE> toClustering(Object...clusterings) {
            FromPartitionKey.super.toClusterings = Arrays.asList(clusterings);
            return new EndBuilder<>();
        }
    }

    public class WithClusterings<TYPE> extends RootClustering<TYPE,WithClusterings<TYPE>> {
        public EndBuilder<TYPE> andClusteringsIn(Object...clusterings) {
            FromPartitionKey.super.andClusteringsIn = Arrays.asList(clusterings);
            return new EndBuilder<>();
        }
    }

    public class EndBuilder<TYPE> extends RootClustering<TYPE,EndBuilder<TYPE>> {

    }


}
