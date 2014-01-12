/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

public class SliceQuery<T> {
	public static final int DEFAULT_LIMIT = 100;
	public static final int DEFAULT_BATCH_SIZE = 100;

	private Class<T> entityClass;
	private EntityMeta meta;
	private List<Object> partitionComponents;
	private List<Object> clusteringsFrom = new ArrayList<Object>();
	private List<Object> clusteringsTo = new ArrayList<Object>();
	private OrderingMode ordering;
	private BoundingMode bounding;
	private ConsistencyLevel consistencyLevel;
	private int batchSize;
	private int limit;
	private boolean limitSet;
	private boolean noComponent;

	public SliceQuery(Class<T> entityClass, EntityMeta meta, List<Object> partitionComponents,
			List<Object> clusteringsFrom, List<Object> clusteringsTo, OrderingMode ordering, BoundingMode bounding,
			ConsistencyLevel consistencyLevel, int limit, int batchSize, boolean limitSet) {

		this.limitSet = limitSet;
		Validator.validateTrue(CollectionUtils.isNotEmpty(partitionComponents),
				"Partition components should be set for slice query for entity class '%s'",
				entityClass.getCanonicalName());

		this.entityClass = entityClass;
		this.meta = meta;

		this.partitionComponents = partitionComponents;
		this.noComponent = clusteringsFrom.isEmpty() && clusteringsTo.isEmpty();

		PropertyMeta idMeta = meta.getIdMeta();

		List<Object> componentsFrom = new ArrayList<Object>();
		componentsFrom.addAll(partitionComponents);
		componentsFrom.addAll(clusteringsFrom);

		this.clusteringsFrom = idMeta.encodeToComponents(componentsFrom);

		List<Object> componentsTo = new ArrayList<>();
		componentsTo.addAll(partitionComponents);
		componentsTo.addAll(clusteringsTo);

		this.clusteringsTo = idMeta.encodeToComponents(componentsTo);

		this.ordering = ordering;
		this.bounding = bounding;
		this.consistencyLevel = consistencyLevel;
		this.limit = limit;
		this.batchSize = batchSize;
	}

	public Class<T> getEntityClass() {
		return entityClass;
	}

	public EntityMeta getMeta() {
		return meta;
	}

	public PropertyMeta getIdMeta() {
		return meta.getIdMeta();
	}

	boolean hasReversedClustering() {
		return getIdMeta().hasReversedComponent();
	}

	public List<Object> getPartitionComponents() {
		return partitionComponents;
	}

	public int partitionComponentsSize() {
		return partitionComponents != null ? partitionComponents.size() : 0;
	}

	public List<Object> getClusteringsFrom() {
		return clusteringsFrom;
	}

	public List<Object> getClusteringsTo() {
		return clusteringsTo;
	}

	public OrderingMode getOrdering() {
		return hasReversedClustering() ? ordering.reverse() : ordering;
	}

	public BoundingMode getBounding() {
		return bounding;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public int getLimit() {
		return limit;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public boolean isLimitSet() {
		return limitSet;
	}

	public boolean hasNoComponent() {
		return noComponent;
	}

}
