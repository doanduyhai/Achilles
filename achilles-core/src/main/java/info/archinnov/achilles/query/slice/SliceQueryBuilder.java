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

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class SliceQueryBuilder<CONTEXT extends PersistenceContext, T> extends RootSliceQueryBuilder<CONTEXT, T> {

	public SliceQueryBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor, CompoundKeyValidator compoundKeyValidator,
			Class<T> entityClass, EntityMeta meta) {
		super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
	}

	/**
	 * Query by partition key components and clustering components<br/>
	 * <br/>
	 * 
	 * @param partitionComponents
	 *            Partition key components
	 * @return SliceShortcutQueryBuilder<T>
	 */
	public SliceShortcutQueryBuilder partitionKey(Object... partitionComponents) {
		super.partitionKeyInternal(partitionComponents);
		return new SliceShortcutQueryBuilder();
	}

    /**
     * Query by indexed columns<br/>
     * <br/>
     *
     * @param indexConditions
     *            list of indexedConditions on indexed columns
     * @param allowFiltering
     *            allow filtering
     * @return SliceShortcutQueryBuilder
     */
	public SliceShortcutQueryBuilder indexedConditions(Collection<IndexCondition> indexConditions, boolean allowFiltering) {
		Validator.validateNotEmpty(indexConditions, "indexConditions should not be empty", null);
		for (IndexCondition indexCondition : indexConditions) {
			super.addCondition(indexCondition);
		}
		super.withAllowFiltering(allowFiltering);
		return new SliceShortcutQueryBuilder();
	}

	/**
	 * Query by from & to embeddedIds<br/>
	 * <br/>
	 * 
	 * @param fromEmbeddedId
	 *            From embeddedId
	 * 
	 * @return SliceFromEmbeddedIdBuilder<T>
	 */
	public SliceFromEmbeddedIdBuilder fromEmbeddedId(Object fromEmbeddedId) {
		Class<?> embeddedIdClass = meta.getIdClass();
		PropertyMeta idMeta = meta.getIdMeta();
		Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromEmbeddedId should be of type '%s'",
				embeddedIdClass.getCanonicalName());
		List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
		List<Object> partitionComponents = idMeta.extractPartitionComponents(components);
		List<Object> clusteringComponents = idMeta.extractClusteringComponents(components);

		super.partitionKeyInternal(partitionComponents);
		this.fromClusteringsInternal(clusteringComponents);

		return new SliceFromEmbeddedIdBuilder();
	}

	/**
	 * Query by from & to embeddedIds<br/>
	 * <br/>
	 * 
	 * @param toEmbeddedId
	 *            To embeddedId
	 * 
	 * @return SliceToEmbeddedIdBuilder
	 */
	public SliceToEmbeddedIdBuilder toEmbeddedId(Object toEmbeddedId) {
		Class<?> embeddedIdClass = meta.getIdClass();
		PropertyMeta idMeta = meta.getIdMeta();
		Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "toEmbeddedId should be of type '%s'",
				embeddedIdClass.getCanonicalName());

		List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
		List<Object> partitionComponents = idMeta.extractPartitionComponents(components);
		List<Object> clusteringComponents = idMeta.extractClusteringComponents(components);

		super.partitionKeyInternal(partitionComponents);
		this.toClusteringsInternal(clusteringComponents);

		return new SliceToEmbeddedIdBuilder();
	}

	public class SliceShortcutQueryBuilder extends DefaultQueryBuilder {

		protected SliceShortcutQueryBuilder() {
		}

		/**
		 * Query using provided consistency level<br/>
		 * <br/>
		 * 
		 * @param consistencyLevel
		 *            consistency level
		 * @return SliceShortcutQueryBuilder
		 */
		@Override
		public SliceShortcutQueryBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
			SliceQueryBuilder.super.consistencyLevelInternal(consistencyLevel);
			return this;
		}

		/**
		 * Set from clustering components<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            From clustering components
		 * 
		 * @return SliceFromClusteringsBuilder
		 */
		public SliceFromClusteringsBuilder fromClusterings(Object... clusteringComponents) {
			SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
			return new SliceFromClusteringsBuilder();
		}

		/**
		 * Set to clustering components<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            To clustering components
		 * 
		 * @return SliceToClusteringsBuilder
		 */
		public SliceToClusteringsBuilder toClusterings(Object... clusteringComponents) {
			SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
			return new SliceToClusteringsBuilder();
		}

        /**
         * Query by indexed columns<br/>
         * <br/>
         *
         * @param indexConditions
         *            list of indexedConditions on indexed columns
         * @param allowFiltering
         *            allow filtering
         * @return SliceShortcutQueryBuilder
         */
        public SliceShortcutQueryBuilder indexedConditions(Collection<IndexCondition> indexConditions,
                                                           boolean allowFiltering) {
            SliceQueryBuilder.this.indexedConditions(indexConditions, allowFiltering);
            return this;
        }

        /**
		 * Set ordering<br/>
		 * <br/>
		 * 
		 * @param ordering
		 *            ordering mode: ASCENDING or DESCENDING
		 * 
		 * @return SliceShortcutQueryBuilder
		 */
		@Override
		public SliceShortcutQueryBuilder ordering(OrderingMode ordering) {
			SliceQueryBuilder.super.ordering(ordering);
			return this;
		}

		/**
		 * Get first n matching entities<br/>
		 * <br/>
		 * 
		 * @param n
		 *            first n matching entities
		 * 
		 * @return list of found entities or empty list
		 */
		@Override
		public List<T> get(int n) {
			return SliceQueryBuilder.super.get(n);
		}

		/**
		 * Get first matching entity, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return first matching entity, filtered by provided clustering
		 *         components if any, or null if no matching entity is found
		 */
		public T getFirstOccurence(Object... clusteringComponents) {
			return SliceQueryBuilder.super.getFirstOccurence(clusteringComponents);
		}

		/**
		 * Get first n matching entities, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param n
		 *            first n matching entities
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return list of n first matching entities, filtered by provided
		 *         clustering components if any, or empty list
		 */
		public List<T> getFirst(int n, Object... clusteringComponents) {
			return SliceQueryBuilder.super.getFirst(n, clusteringComponents);
		}

		/**
		 * Get last matching entity, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return last matching entity, filtered by provided clustering
		 *         components if any, or null if no matching entity is found
		 */
		public T getLastOccurence(Object... clusteringComponents) {
			return SliceQueryBuilder.super.getLastOccurence(clusteringComponents);
		}

		/**
		 * Get last n matching entities, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param n
		 *            last n matching entities
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return list of last n matching entities, filtered by provided
		 *         clustering components if any, or empty list
		 */
		public List<T> getLast(int n, Object... clusteringComponents) {
			return SliceQueryBuilder.super.getLast(n, clusteringComponents);
		}

		/**
		 * Get entities iterator, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return iterator on found entities
		 */
		public Iterator<T> iterator(Object... clusteringComponents) {
			return SliceQueryBuilder.super.iteratorWithComponents(clusteringComponents);
		}

		/**
		 * Get entities iterator, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param batchSize
		 *            batch loading size for iterator
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 * 
		 * @return iterator on found entities
		 */
		public Iterator<T> iterator(int batchSize, Object... clusteringComponents) {
			return SliceQueryBuilder.super.iteratorWithComponents(batchSize, clusteringComponents);
		}

		/**
		 * Remove first n entities, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param n
		 *            first n entities
		 */
		@Override
		public void remove(int n) {
			SliceQueryBuilder.super.remove(n);
		}

		/**
		 * Remove first matching entity, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 */
		public void removeFirstOccurence(Object... clusteringComponents) {
			SliceQueryBuilder.super.removeFirstOccurence(clusteringComponents);
		}

		/**
		 * Remove first n matching entities, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param n
		 *            first n matching entities
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 */
		public void removeFirst(int n, Object... clusteringComponents) {
			SliceQueryBuilder.super.removeFirst(n, clusteringComponents);
		}

		/**
		 * Remove last matching entity, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 */
		public void removeLastOccurence(Object... clusteringComponents) {
			SliceQueryBuilder.super.removeLastOccurence(clusteringComponents);
		}

		/**
		 * Remove last n matching entities, using ASCENDING order<br/>
		 * <br/>
		 * 
		 * @param n
		 *            last n matching entities
		 * 
		 * @param clusteringComponents
		 *            optional clustering components for filtering
		 */
		public void removeLast(int n, Object... clusteringComponents) {
			SliceQueryBuilder.super.removeLast(n, clusteringComponents);
		}
	}

	public class SliceFromEmbeddedIdBuilder {
		protected SliceFromEmbeddedIdBuilder() {
		}

		/**
		 * Set to embeddedId<br/>
		 * <br/>
		 * 
		 * @param toEmbeddedId
		 *            To embeddedId
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder toEmbeddedId(Object toEmbeddedId) {
			SliceQueryBuilder.this.toEmbeddedId(toEmbeddedId);
			return new DefaultQueryBuilder();
		}

        /**
         * Query by indexed columns<br/>
         * <br/>
         *
         * @param indexConditions
         *            list of indexedConditions on indexed columns
         * @param allowFiltering
         *            allow filtering
         * @return SliceFromEmbeddedIdBuilder
         */
        public SliceFromEmbeddedIdBuilder indexedConditions(Collection<IndexCondition> indexConditions,
                                                            boolean allowFiltering) {
            SliceQueryBuilder.this.indexedConditions(indexConditions, allowFiltering);
            return this;
        }

	}

	public class SliceToEmbeddedIdBuilder {
		protected SliceToEmbeddedIdBuilder() {
		}

		/**
		 * Set from embeddedId<br/>
		 * <br/>
		 * 
		 * @param fromEmbeddedId
		 *            From embeddedId
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder fromEmbeddedId(Object fromEmbeddedId) {
			SliceQueryBuilder.this.fromEmbeddedId(fromEmbeddedId);
			return new DefaultQueryBuilder();
		}

        /**
         * Query by indexed columns<br/>
         * <br/>
         *
         * @param indexConditions
         *            list of indexedConditions on indexed columns
         * @param allowFiltering
         *            allow filtering
         * @return SliceToEmbeddedIdBuilder
         */
        public SliceToEmbeddedIdBuilder indexedConditions(Collection<IndexCondition> indexConditions,
                                                          boolean allowFiltering) {
            SliceQueryBuilder.this.indexedConditions(indexConditions, allowFiltering);
            return this;
        }
	}

	public class SliceFromClusteringsBuilder extends DefaultQueryBuilder {

		public SliceFromClusteringsBuilder() {
		}

		/**
		 * Set to clustering components<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            To clustering components
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder toClusterings(Object... clusteringComponents) {
			SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
			return new DefaultQueryBuilder();
		}

        /**
         * Query by indexed columns<br/>
         * <br/>
         *
         * @param indexConditions
         *            list of indexedConditions on indexed columns
         * @param allowFiltering
         *            allow filtering
         * @return SliceFromClusteringsBuilder
         */
        public SliceFromClusteringsBuilder indexedConditions(Collection<IndexCondition> indexConditions,
                                                             boolean allowFiltering) {
            SliceQueryBuilder.this.indexedConditions(indexConditions, allowFiltering);
            return this;
        }
	}

	public class SliceToClusteringsBuilder extends DefaultQueryBuilder {

		public SliceToClusteringsBuilder() {
		}

		/**
		 * Set from clustering components<br/>
		 * <br/>
		 * 
		 * @param clusteringComponents
		 *            From clustering components
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder fromClusterings(Object... clusteringComponents) {
			SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
			return new DefaultQueryBuilder();
		}

        /**
         * Query by indexed columns<br/>
         * <br/>
         *
         * @param indexConditions
         *            list of indexedConditions on indexed columns
         * @param allowFiltering
         *            allow filtering
         * @return SliceToClusteringsBuilder
         */
        public SliceToClusteringsBuilder indexedConditions(Collection<IndexCondition> indexConditions,
                                                           boolean allowFiltering) {
            SliceQueryBuilder.this.indexedConditions(indexConditions, allowFiltering);
            return this;
        }
	}

	public class DefaultQueryBuilder {

		protected DefaultQueryBuilder() {
		}

		/**
		 * Set ordering<br/>
		 * <br/>
		 * 
		 * @param ordering
		 *            ordering mode: ASCENDING or DESCENDING
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder ordering(OrderingMode ordering) {
			SliceQueryBuilder.super.ordering(ordering);
			return this;
		}

		/**
		 * Set bounding mode<br/>
		 * <br/>
		 * 
		 * @param boundingMode
		 *            bounding mode: ASCENDING or DESCENDING
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder bounding(BoundingMode boundingMode) {
			SliceQueryBuilder.super.bounding(boundingMode);
			return this;
		}

		/**
		 * Set consistency level<br/>
		 * <br/>
		 * 
		 * @param consistencyLevel
		 *            consistency level:
		 *            ONE,TWO,THREE,QUORUM,LOCAL_QUORUM,EACH_QUORUM or ALL
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
			SliceQueryBuilder.super.consistencyLevelInternal(consistencyLevel);
			return this;
		}

		/**
		 * Set limit<br/>
		 * <br/>
		 * 
		 * @param limit
		 *            limit to the number of returned rows
		 * 
		 * @return DefaultQueryBuilder
		 */
		public DefaultQueryBuilder limit(int limit) {
			SliceQueryBuilder.super.limit(limit);
			return this;
		}

		/**
		 * Get entities<br/>
		 * <br/>
		 * 
		 * 
		 * @return List<T>
		 */
		public List<T> get() {
			return SliceQueryBuilder.super.get();
		}

		/**
		 * Get first n entities<br/>
		 * <br/>
		 * 
		 * 
		 * @return List<T>
		 */
		public List<T> get(int n) {
			return SliceQueryBuilder.super.get(n);
		}

		/**
		 * Iterator on entities<br/>
		 * <br/>
		 * 
		 * 
		 * @return Iterator<T>
		 */
		public Iterator<T> iterator() {
			return SliceQueryBuilder.super.iterator();
		}

		/**
		 * Iterator on entities with batchSize<br/>
		 * <br/>
		 * 
		 * @param batchSize
		 *            maximum number of rows to fetch on each batch
		 * 
		 * @return Iterator<T>
		 */
		public Iterator<T> iterator(int batchSize) {
			return SliceQueryBuilder.super.iterator(batchSize);
		}

		/**
		 * Remove matched entities<br/>
		 * <br/>
		 * 
		 * @return Iterator<T>
		 */
		public void remove() {
			SliceQueryBuilder.super.remove();
		}

		/**
		 * Remove first n matched entities<br/>
		 * <br/>
		 * 
		 * @param n
		 *            first n matched entities
		 * 
		 * @return Iterator<T>
		 */
		public void remove(int n) {
			SliceQueryBuilder.super.remove(n);
		}
	}
}
