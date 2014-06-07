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
import static java.util.Arrays.asList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class SliceQueryRoot<TYPE, T extends SliceQueryRoot<TYPE, T>> {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryRoot.class);

    protected final SliceQueryExecutor sliceQueryExecutor;
    protected final Class<TYPE> entityClass;
    protected final EntityMeta meta;
    protected final SliceQueryProperties<TYPE> properties;


    protected SliceQueryRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
        this.properties = SliceQueryProperties.builder(meta, entityClass, sliceType);
    }

    protected void withPartitionComponentsInternal(Object... partitionKeyComponents) {
        log.trace("Add partition key components {}", partitionKeyComponents);
        Validator.validateNotEmpty(partitionKeyComponents, "Partition key components should not be empty");
        meta.forSliceQuery().validatePartitionComponents(partitionKeyComponents);
        SliceQueryRoot.this.properties.partitionKeys(asList(partitionKeyComponents));
        SliceQueryRoot.this.properties.partitionKeysName(meta.forSliceQuery().getPartitionKeysName(partitionKeyComponents.length));
    }

    protected void withPartitionComponentsINInternal(Object... partitionKeyComponentsIn) {
        log.trace("Add partition key components for IN clause {}", partitionKeyComponentsIn);

        meta.forSliceQuery().validatePartitionComponentsIn(partitionKeyComponentsIn);
        SliceQueryRoot.this.properties.andPartitionKeysIn(asList(partitionKeyComponentsIn));
        SliceQueryRoot.this.properties.lastPartitionKeyName(meta.forSliceQuery().getLastPartitionKeyName());
    }

    protected void andPartitionKeysINInternal(Object... partitionKeyComponentsIn) {
        log.trace("Add partition key components for IN clause {}", partitionKeyComponentsIn);

        final List<Object> partitionKeys = SliceQueryRoot.this.properties.getPartitionKeys();
        final int correctPartitionKeysSize = this.meta.forSliceQuery().getPartitionKeysSize() - 1;

        Validator.validateNotEmpty(partitionKeys, "Before adding partition key components for IN clause, you should define first partition key components for query using withPartitionKeys(Object... partitionKeyComponentsIn)");
        Validator.validateNotEmpty(partitionKeyComponentsIn, "Partition key components for IN clause should not be empty");
        Validator.validateTrue(partitionKeys.size() == correctPartitionKeysSize, "To use the IN clause, you must provide '%s' partition keys components first", correctPartitionKeysSize);

        meta.forSliceQuery().validatePartitionComponentsIn(partitionKeyComponentsIn);
        SliceQueryRoot.this.properties.andPartitionKeysIn(asList(partitionKeyComponentsIn));
        SliceQueryRoot.this.properties.lastPartitionKeyName(meta.forSliceQuery().getLastPartitionKeyName());
    }

    protected void fromClusteringsInternal(Object... clusteringKeys) {
        log.trace("Add from clustering components {}", clusteringKeys);
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.forSliceQuery().validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.fromClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.fromClusteringKeysName(meta.forSliceQuery().getClusteringKeysName(clusteringKeys.length));
    }

    protected void toClusteringsInternal(Object... clusteringKeys) {
        log.trace("Add to clustering components {}", clusteringKeys);
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.forSliceQuery().validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.toClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.toClusteringKeysName(meta.forSliceQuery().getClusteringKeysName(clusteringKeys.length));
    }

    protected void withClusteringsInternal(Object... clusteringKeys) {
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.forSliceQuery().validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.withClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.withClusteringKeysName(meta.forSliceQuery().getClusteringKeysName(clusteringKeys.length));
    }

    protected void andClusteringsInInternal(Object... clusteringKeys) {
        final List<Object> withClusteringKeys = SliceQueryRoot.this.properties.getWithClusteringKeys();
        final int correctWithClusteringKeysSize = this.meta.forSliceQuery().getClusteringKeysSize() - 1;

        Validator.validateNotEmpty(withClusteringKeys, "Before adding clustering key components for IN clause, you should define first clustering key components for query using withClusteringKeys(Object... partitionKeys)");
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components for IN clause should not be empty");
        Validator.validateTrue(withClusteringKeys.size() == correctWithClusteringKeysSize, "To use the IN clause, you must provide '%s' clustering keys components first", correctWithClusteringKeysSize);

        meta.forSliceQuery().validateClusteringComponentsIn(clusteringKeys);
        SliceQueryRoot.this.properties.andClusteringKeysIn(asList(clusteringKeys));
        SliceQueryRoot.this.properties.lastClusteringKeyName(meta.forSliceQuery().getLastClusteringKeyName());
    }

    /**
     *
     * Provide a consistency level for SELECT/DELETE statement
     *
     * @param consistencyLevel
     * @return Slice DSL
     */
    public T withConsistency(ConsistencyLevel consistencyLevel) {
        this.properties.consistency(consistencyLevel);
        return getThis();
    }

    public T withAsyncListeners(FutureCallback<Object>... asyncListeners) {
        this.properties.asyncListeners(asyncListeners);
        return getThis();
    }

    protected abstract T getThis();

    protected List<TYPE> getInternal() {
        return this.sliceQueryExecutor.get(this.properties);
    }


    protected AchillesFuture<List<TYPE>> asyncGetInternal() {
        return this.sliceQueryExecutor.asyncGet(this.properties);
    }

    protected AchillesFuture<TYPE> asyncGetOneInternal() {
        return this.sliceQueryExecutor.asyncGetOne(this.properties);
    }

    protected Iterator<TYPE> iteratorInternal() {
        return this.sliceQueryExecutor.iterator(this.properties);
    }

    protected AchillesFuture<Iterator<TYPE>> asyncIteratorInternal() {
        return this.sliceQueryExecutor.asyncIterator(this.properties);
    }

    protected void deleteInternal() {
        this.sliceQueryExecutor.delete(this.properties);
    }

    protected AchillesFuture<Empty> asyncDeleteInternal() {
        return this.sliceQueryExecutor.asyncDelete(this.properties);
    }
}
