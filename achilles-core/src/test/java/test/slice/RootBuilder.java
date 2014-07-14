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
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;

public abstract class RootBuilder<TYPE,T extends RootBuilder<TYPE,T>> implements CommonProperties<T>{

    public static final int DEFAULT_LIMIT = 100;
    public static final int DEFAULT_BATCH_SIZE = 100;

    protected Class<TYPE> entityClass;
    protected BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;
    protected OrderingMode orderingMode = OrderingMode.ASCENDING;

    protected int limit = DEFAULT_LIMIT;
    protected int batchSize = DEFAULT_BATCH_SIZE;

    protected ConsistencyLevel consistencyLevel;

    protected List<Object> partitionKeys;
    protected List<Object> partitionKeysIn;
    protected List<Object> fromClusterings;
    protected List<Object> toClusterings;
    protected List<Object> withClusterings;
    protected List<Object> andClusteringsIn;


    @Override
    public T inclusiveBounds() {
        this.boundingMode = BoundingMode.INCLUSIVE_BOUNDS;
        return (T) this;
    }

    @Override
    public T exclusiveBounds() {
        this.boundingMode = BoundingMode.EXCLUSIVE_BOUNDS;
        return (T) this;
    }

    @Override
    public T fromInclusiveToExclusiveBounds() {
        this.boundingMode = BoundingMode.INCLUSIVE_START_BOUND_ONLY;
        return (T) this;
    }

    @Override
    public T fromExclusiveToInclusiveBounds() {
        this.boundingMode = BoundingMode.INCLUSIVE_END_BOUND_ONLY;
        return (T) this;
    }

    @Override
    public T orderByAscending() {
        this.orderingMode = OrderingMode.ASCENDING;
        return (T) this;
    }

    @Override
    public T orderByDescending() {
        this.orderingMode = OrderingMode.DESCENDING;
        return (T) this;
    }

    @Override
    public T limit(int limit) {
        this.limit = limit;
        return (T) this;
    }

    @Override
    public T withConsistency(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return (T) this;
    }

    public void check() {
        System.out.println("partitionKeys = "+RootBuilder.this.partitionKeys);
        System.out.println("partitionKeysIn = "+RootBuilder.this.partitionKeysIn);
        System.out.println("fromClusterings = "+RootBuilder.this.fromClusterings);
        System.out.println("toClusterings = "+RootBuilder.this.toClusterings);
        System.out.println("withClusterings = "+RootBuilder.this.withClusterings);
        System.out.println("andClusteringsIn = "+RootBuilder.this.andClusteringsIn);
        System.out.println("limit = "+RootBuilder.this.limit);
        System.out.println("consistency = "+RootBuilder.this.consistencyLevel);
        System.out.println("bounds = "+RootBuilder.this.boundingMode);
        System.out.println("order = "+RootBuilder.this.orderingMode);
    }
}
