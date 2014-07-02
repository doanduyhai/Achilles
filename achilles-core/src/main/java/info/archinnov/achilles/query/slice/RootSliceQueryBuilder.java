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

import static info.archinnov.achilles.query.slice.SliceQuery.*;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RootSliceQueryBuilder<T> {
	private static final Logger log = LoggerFactory.getLogger(RootSliceQueryBuilder.class);

	protected SliceQueryExecutor sliceQueryExecutor;
	protected Class<T> entityClass;
	protected EntityMeta meta;

	protected List<Object> partitionComponents = new ArrayList<>();
	private PropertyMeta idMeta;
	private List<Object> fromClusterings = new ArrayList<>();
	private List<Object> toClusterings = new ArrayList<>();
	private OrderingMode ordering = OrderingMode.ASCENDING;
	private BoundingMode bounding = BoundingMode.INCLUSIVE_BOUNDS;
	private ConsistencyLevel consistencyLevel;
	private int limit = DEFAULT_LIMIT;
	private int batchSize = DEFAULT_BATCH_SIZE;
	private boolean limitHasBeenSet = false;
	private boolean orderingHasBeenSet = false;

	RootSliceQueryBuilder(SliceQueryExecutor sliceQueryExecutor, Class<T> entityClass, EntityMeta meta) {
		this.sliceQueryExecutor = sliceQueryExecutor;
		this.entityClass = entityClass;
		this.meta = meta;
		this.idMeta = meta.getIdMeta();
	}

	protected RootSliceQueryBuilder<T> partitionComponentsInternal(List<Object> partitionComponents) {
		log.trace("Add partition key components {}", partitionComponents);
		idMeta.validatePartitionComponents(partitionComponents);
		if (this.partitionComponents.size() > 0) {
			Validator.validateTrue(this.partitionComponents.size() == partitionComponents.size(),
					"Partition components '%s' do not match previously set values '%s'", partitionComponents,
					this.partitionComponents);
			for (int i = 0; i < partitionComponents.size(); i++) {
				Validator.validateTrue(this.partitionComponents.get(i).equals(partitionComponents.get(i)),
						"Partition components '%s' do not match previously set values '%s'", partitionComponents,
						this.partitionComponents);
			}
		}
		this.partitionComponents = partitionComponents;
		return this;
	}

	protected RootSliceQueryBuilder<T> partitionComponentsInternal(Object... partitionComponents) {
		this.partitionComponentsInternal(Arrays.asList(partitionComponents));
		return this;
	}

	protected RootSliceQueryBuilder<T> fromClusteringsInternal(List<Object> clusteringComponents) {
		log.trace("Add clustering components {}", clusteringComponents);
		idMeta.validateClusteringComponents(clusteringComponents);
		fromClusterings = clusteringComponents;
		return this;
	}

	protected RootSliceQueryBuilder<T> fromClusteringsInternal(Object... clusteringComponents) {
		this.fromClusteringsInternal(Arrays.asList(clusteringComponents));
		return this;
	}

	protected RootSliceQueryBuilder<T> toClusteringsInternal(List<Object> clusteringComponents) {
		log.trace("Add clustering components {}", clusteringComponents);
		idMeta.validateClusteringComponents(clusteringComponents);
		toClusterings = clusteringComponents;
		return this;
	}

	protected RootSliceQueryBuilder<T> toClusteringsInternal(Object... clusteringComponents) {
		this.toClusteringsInternal(Arrays.asList(clusteringComponents));
		return this;
	}

	protected RootSliceQueryBuilder<T> ordering(OrderingMode ordering) {
		Validator.validateNotNull(ordering, "Ordering mode for slice query for entity '%s' should not be null",
				meta.getClassName());
		this.ordering = ordering;
		orderingHasBeenSet = true;
		return this;
	}

	protected RootSliceQueryBuilder<T> bounding(BoundingMode boundingMode) {
		Validator.validateNotNull(boundingMode, "Bounding mode for slice query for entity '%s' should not be null",
				meta.getClassName());
		bounding = boundingMode;

		return this;
	}

	protected RootSliceQueryBuilder<T> consistencyLevelInternal(ConsistencyLevel consistencyLevel) {
		Validator.validateNotNull(consistencyLevel,
				"ConsistencyLevel for slice query for entity '%s' should not be null", meta.getClassName());
		this.consistencyLevel = consistencyLevel;

		return this;
	}

	protected RootSliceQueryBuilder<T> limit(int limit) {
		this.limit = limit;
		limitHasBeenSet = true;
		return this;
	}

	protected List<T> get() {
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.get(clusteredQuery);
	}

	protected List<T> get(int n) {
		limit = n;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.get(clusteredQuery);
	}

	protected T getFirstOccurence(Object... clusteringComponents) {
		log.trace("Get first result using clustering components {}", clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getFirst()");
		limit = 1;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		List<T> result = sliceQueryExecutor.get(clusteredQuery);
		if (result.isEmpty())
			return null;
		else
			return result.get(0);
	}

	protected List<T> getFirst(int n, Object... clusteringComponents) {
		log.trace("Get first {} results using clustering components {}", n, clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getFirst(int n)");
		limit = n;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.get(clusteredQuery);
	}

	protected T getLastOccurence(Object... clusteringComponents) {
		log.trace("Get last result using clustering components {}", clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling getLast()");
		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getLast()");
		limit = 1;
		limitHasBeenSet = true;
		ordering = OrderingMode.DESCENDING;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		List<T> result = sliceQueryExecutor.get(clusteredQuery);
		if (result.isEmpty())
			return null;
		else
			return result.get(0);
	}

	protected List<T> getLast(int n, Object... clusteringComponents) {
		log.trace("Get last {} results using clustering components {}", n, clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(orderingHasBeenSet,
				"You should not set 'ordering' parameter when calling getLast(int n)");
		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getLast(int n)");
		limit = n;
		limitHasBeenSet = true;
		ordering = OrderingMode.DESCENDING;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.get(clusteredQuery);
	}

	protected Iterator<T> iterator() {
		log.trace("Build iterator for slice query");
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.iterator(clusteredQuery);
	}

	protected Iterator<T> iteratorWithComponents(Object... clusteringComponents) {
		log.trace("Build iterator for slice query with clustering components {}", clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.iterator(clusteredQuery);
	}

	protected Iterator<T> iterator(int batchSize) {
		log.trace("Build iterator for slice query with batch size {}", batchSize);
		this.batchSize = batchSize;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.iterator(clusteredQuery);
	}

	protected Iterator<T> iteratorWithComponents(int batchSize, Object... clusteringComponents) {
		log.trace("Build iterator for slice query with clustering components {} and batch size {}",
				clusteringComponents, batchSize);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);
		this.batchSize = batchSize;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		return sliceQueryExecutor.iterator(clusteredQuery);
	}

	protected void remove() {
		log.trace("Slice remove");
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected void remove(int n) {
		log.trace("Slice remove {} entities", n);
		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling remove(int n)");
		limit = n;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected void removeFirstOccurence(Object... clusteringComponents) {
		log.trace("Slice remove first matching entity with clustering components {}", clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeFirst()");
		limit = 1;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected void removeFirst(int n, Object... clusteringComponents) {
		log.trace("Slice remove first {} matching entities with clustering components {}", n, clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator
				.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeFirst(int n)");
		limit = n;
		limitHasBeenSet = true;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected void removeLastOccurence(Object... clusteringComponents) {
		log.trace("Slice remove last matching entity with clustering components {}", clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator
				.validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling removeLast()");
		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeLast()");
		limit = 1;
		limitHasBeenSet = true;
		ordering = OrderingMode.DESCENDING;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected void removeLast(int n, Object... clusteringComponents) {
		log.trace("Slice remove last {} matching entities with clustering components {}", n, clusteringComponents);
		fromClusteringsInternal(clusteringComponents);
		toClusteringsInternal(clusteringComponents);

		Validator.validateFalse(orderingHasBeenSet,
				"You should not set 'ordering' parameter when calling removeLast(int n)");
		Validator.validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeLast(int n)");
		limit = n;
		limitHasBeenSet = true;
		ordering = OrderingMode.DESCENDING;
		SliceQuery<T> clusteredQuery = buildClusterQuery();
		sliceQueryExecutor.remove(clusteredQuery);
	}

	protected SliceQuery<T> buildClusterQuery() {
		return new SliceQuery<>(entityClass, meta, partitionComponents, fromClusterings, toClusterings, ordering,
				bounding, consistencyLevel, limit, batchSize, limitHasBeenSet);
	}
}
