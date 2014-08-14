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

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;

public abstract class SliceQueryRoot<TYPE, T extends SliceQueryRoot<TYPE, T>> {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryRoot.class);

    protected final SliceQueryExecutor sliceQueryExecutor;
    protected final Class<TYPE> entityClass;
    protected final EntityMeta meta;
    protected final SliceQueryProperties properties;


    protected SliceQueryRoot(SliceQueryExecutor sliceQueryExecutor, Class<TYPE> entityClass, EntityMeta meta, SliceType sliceType) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
        this.properties = SliceQueryProperties.builder(meta, entityClass, sliceType);
    }



    protected void withPartitionComponentsInternal(Object... partitionKeyComponents) {
        log.trace("Add partition key components {}", partitionKeyComponents);
        Validator.validateNotEmpty(partitionKeyComponents, "Partition key components should not be empty");
        meta.validatePartitionComponents(partitionKeyComponents);
        SliceQueryRoot.this.properties.partitionKeys(asList(partitionKeyComponents));
        SliceQueryRoot.this.properties.partitionKeysName(meta.getPartitionKeysName(partitionKeyComponents.length));
    }

    protected void withPartitionComponentsINInternal(Object... partitionKeyComponentsIn) {
        log.trace("Add partition key components for IN clause {}", partitionKeyComponentsIn);

        meta.validatePartitionComponentsIn(partitionKeyComponentsIn);
        SliceQueryRoot.this.properties.partitionKeysIn(asList(partitionKeyComponentsIn));
        SliceQueryRoot.this.properties.lastPartitionKeyName(meta.getLastPartitionKeyName());
    }

    protected void andPartitionKeysINInternal(Object... partitionKeyComponentsIn) {
        log.trace("Add partition key components for IN clause {}", partitionKeyComponentsIn);

        final List<Object> partitionKeys = SliceQueryRoot.this.properties.getPartitionKeys();
        final int correctPartitionKeysSize = this.meta.getPartitionKeysSize() - 1;

        Validator.validateNotEmpty(partitionKeys, "Before adding partition key components for IN clause, you should define first partition key components for query using withPartitionKeys(Object... partitionKeyComponentsIn)");
        Validator.validateNotEmpty(partitionKeyComponentsIn, "Partition key components for IN clause should not be empty");
        Validator.validateTrue(partitionKeys.size() == correctPartitionKeysSize, "To use the IN clause, you must provide '%s' partition keys components first", correctPartitionKeysSize);

        meta.validatePartitionComponentsIn(partitionKeyComponentsIn);
        SliceQueryRoot.this.properties.partitionKeysIn(asList(partitionKeyComponentsIn));
        SliceQueryRoot.this.properties.lastPartitionKeyName(meta.getLastPartitionKeyName());
    }

    protected void fromClusteringsInternal(Object... clusteringKeys) {
        log.trace("Add from clustering components {}", clusteringKeys);
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.fromClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.fromClusteringKeysName(meta.getClusteringKeysName(clusteringKeys.length));
    }

    protected void toClusteringsInternal(Object... clusteringKeys) {
        log.trace("Add to clustering components {}", clusteringKeys);
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.toClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.toClusteringKeysName(meta.getClusteringKeysName(clusteringKeys.length));
    }

    protected void withClusteringsInternal(Object... clusteringKeys) {
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components should not be empty");
        meta.validateClusteringComponents(clusteringKeys);
        SliceQueryRoot.this.properties.withClusteringKeys(asList(clusteringKeys));
        SliceQueryRoot.this.properties.withClusteringKeysName(meta.getClusteringKeysName(clusteringKeys.length));
    }

    protected void andClusteringsInInternal(Object... clusteringKeys) {
        final List<Object> withClusteringKeys = SliceQueryRoot.this.properties.getWithClusteringKeys();
        final int correctWithClusteringKeysSize = this.meta.getClusteringKeysSize() - 1;

        Validator.validateNotEmpty(withClusteringKeys, "Before adding clustering key components for IN clause, you should define first clustering key components for query using withClusteringKeys(Object... partitionKeys)");
        Validator.validateNotEmpty(clusteringKeys, "Clustering key components for IN clause should not be empty");
        Validator.validateTrue(withClusteringKeys.size() == correctWithClusteringKeysSize, "To use the IN clause, you must provide '%s' clustering keys components first", correctWithClusteringKeysSize);

        meta.validateClusteringComponentsIn(clusteringKeys);
        SliceQueryRoot.this.properties.andClusteringKeysIn(asList(clusteringKeys));
        SliceQueryRoot.this.properties.lastClusteringKeyName(meta.getLastClusteringKeyName());
    }
    
    protected void withIndexConditionInternal(IndexCondition indexCondition) {
        log.trace("Add index condition {}", indexCondition);
        SliceQueryRoot.this.properties.withIndexCondition(indexCondition);
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

    protected abstract T getThis();

    protected List<TYPE> getInternal() {
        return this.sliceQueryExecutor.get(this.properties);
    }

    protected Iterator<TYPE> iteratorInternal() {
        return this.sliceQueryExecutor.iterator(this.properties);
    }

    protected void deleteInternal() {
        this.sliceQueryExecutor.delete(this.properties);
    }

}
