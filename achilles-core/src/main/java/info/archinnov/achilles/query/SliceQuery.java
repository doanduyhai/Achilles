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
package info.archinnov.achilles.query;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

public class SliceQuery<T> {
	public static final int DEFAULT_LIMIT = 100;
	public static final int DEFAULT_BATCH_SIZE = 100;

	private Class<T> entityClass;
	private EntityMeta meta;
	private Object partitionKey;
	private List<Object> clusteringsFrom;
	private List<Object> clusteringsTo;
	private OrderingMode ordering;
	private BoundingMode bounding;
	private ConsistencyLevel consistencyLevel;
	private int batchSize;
	private int limit;
	private boolean limitSet;
	private boolean noComponent;
	private boolean allowFiltering;
	private Collection<IndexCondition> indexConditions;

	public SliceQuery(Class<T> entityClass, EntityMeta meta, Object partitionKey, Object[] clusteringsFrom,
			Object[] clusteringsTo, OrderingMode ordering, BoundingMode bounding, ConsistencyLevel consistencyLevel,
			int limit, int batchSize, boolean limitSet, boolean allowfiltering,
			Collection<IndexCondition> indexConditions) {

		this.limitSet = limitSet;
		this.entityClass = entityClass;
		this.meta = meta;

		this.partitionKey = partitionKey;
		this.noComponent = clusteringsFrom == null && clusteringsTo == null;
		this.indexConditions = indexConditions;
		this.ordering = ordering;
		this.bounding = bounding;
		this.consistencyLevel = consistencyLevel;
		this.limit = limit;
		this.batchSize = batchSize;
		this.allowFiltering = allowfiltering;
		if (!hasIndexConditions()) {
			Validator
					.validateNotNull(partitionKey, "Partition key should be set for slice query for entity class '%s'",
							entityClass.getCanonicalName());
			PropertyMeta idMeta = meta.getIdMeta();
			List<Object> componentsFrom = Arrays.<Object> asList(ArrayUtils.add(clusteringsFrom, 0, partitionKey));
			this.clusteringsFrom = idMeta.encodeToComponents(componentsFrom);
			List<Object> componentsTo = Arrays.<Object> asList(ArrayUtils.add(clusteringsTo, 0, partitionKey));
			this.clusteringsTo = idMeta.encodeToComponents(componentsTo);
		}

	}

	public Class<T> getEntityClass() {
		return entityClass;
	}

	public EntityMeta getMeta() {
		return meta;
	}

	public Object getPartitionKey() {
		return partitionKey;
	}

	public List<Object> getClusteringsFrom() {
		return clusteringsFrom;
	}

	public List<Object> getClusteringsTo() {
		return clusteringsTo;
	}

	public OrderingMode getOrdering() {
		return ordering;
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

	public boolean isAllowFiltering() {
		return allowFiltering;
	}

	public boolean hasNoComponent() {
		return noComponent;
	}

	public Collection<IndexCondition> getIndexConditions() {
		return indexConditions;
	}

	public boolean hasIndexConditions() {
		return indexConditions != null && indexConditions.size() > 0;
	}

}
