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

import static info.archinnov.achilles.consistency.CQLConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.compound.CQLCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.utils.Pair;

import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CQLSliceQuery<T> {

	private SliceQuery<T> sliceQuery;
	private List<Object> fixedComponents;
	private Object lastStartComp;
	private Object lastEndComp;
	private CQLCompoundKeyValidator validator = new CQLCompoundKeyValidator();
	private ConsistencyLevel defaultReadLevel;

	public CQLSliceQuery(SliceQuery<T> sliceQuery, ConsistencyLevel defaultReadLevel) {

        this.sliceQuery = sliceQuery;
        this.defaultReadLevel = defaultReadLevel;

        validateClusteringComponents(sliceQuery);

        this.fixedComponents = determineFixedComponents(sliceQuery);
        Pair<Object, Object> lastComponents = determineLastComponents(sliceQuery);
        this.lastStartComp = lastComponents.left;
        this.lastEndComp = lastComponents.right;
	}

	public List<Object> getFixedComponents() {
		return fixedComponents;
	}

	public Object getLastStartComponent() {
		return lastStartComp;
	}

	public Object getLastEndComponent() {
		return lastEndComp;
	}

	public int getLimit() {
		return sliceQuery.getLimit();
	}

	public boolean isAllowFiltering() {
		return sliceQuery.isAllowFiltering();
	}

	public com.datastax.driver.core.ConsistencyLevel getConsistencyLevel() {
		ConsistencyLevel consistencyLevel = sliceQuery.getConsistencyLevel() == null ? defaultReadLevel : sliceQuery
				.getConsistencyLevel();
		return getCQLLevel(consistencyLevel);
	}

	public BoundingMode getBounding() {
		return sliceQuery.getBounding();
	}

	public OrderingMode getAchillesOrdering() {
		return sliceQuery.getOrdering();
	}

	public Ordering getCQLOrdering() {
		OrderingMode ordering = sliceQuery.getOrdering();
		String orderingComponentName = sliceQuery.getMeta().getIdMeta().getOrderingComponent();
        if(!hasIndexConditions()) {
			if (ordering.isReverse()) {
				return QueryBuilder.desc(orderingComponentName);
			} else {
				return QueryBuilder.asc(orderingComponentName);
			}
        }
		return null;
	}

	public List<String> getComponentNames() {
		return sliceQuery.getMeta().getIdMeta().getComponentNames();
	}

	public String getVaryingComponentName() {
		return sliceQuery.getMeta().getIdMeta().getComponentNames().get(fixedComponents.size());
	}

	public Class<?> getVaryingComponentClass() {
		return sliceQuery.getMeta().getIdMeta().getComponentClasses().get(fixedComponents.size());
	}

	public EntityMeta getMeta() {
		return sliceQuery.getMeta();
	}

	public Class<T> getEntityClass() {
		return sliceQuery.getEntityClass();
	}

	public int getBatchSize() {
		return sliceQuery.getBatchSize();
	}

	public Collection<IndexCondition> getIndexConditions() {
		return sliceQuery.getIndexConditions();
	}

	public boolean hasIndexConditions() {
		return sliceQuery.hasIndexConditions();
	}

	private void validateClusteringComponents(SliceQuery<T> sliceQuery) {
        validator.validateComponentsForSliceQuery(sliceQuery.getClusteringsFrom(), sliceQuery.getClusteringsTo(),
            sliceQuery.getOrdering());
	}

	private List<Object> determineFixedComponents(SliceQuery<T> sliceQuery) {
		List<Object> fixedComponents = new ArrayList<Object>();
		List<Object> startComponents = sliceQuery.getClusteringsFrom();
		List<Object> endComponents = sliceQuery.getClusteringsTo();


		int startIndex = validator.getLastNonNullIndex(startComponents);
		int endIndex = validator.getLastNonNullIndex(endComponents);

		int minIndex = Math.min(startIndex, endIndex);

		if (startIndex == endIndex) {
			for (int i = 0; i <= minIndex && startComponents.get(i).equals(endComponents.get(i)); i++) {
				fixedComponents.add(startComponents.get(i));
			}
		} else {
			for (int i = 0; i <= minIndex; i++) {
				fixedComponents.add(startComponents.get(i));
			}
		}

		return fixedComponents;
	}

	private Pair<Object, Object> determineLastComponents(SliceQuery<T> sliceQuery) {
		Object lastStartComp;
		Object lastEndComp;

		List<Object> startComponents = sliceQuery.getClusteringsFrom();
		List<Object> endComponents = sliceQuery.getClusteringsTo();

		int startIndex = validator.getLastNonNullIndex(startComponents);
		int endIndex = validator.getLastNonNullIndex(endComponents);

		if (startIndex == endIndex && !startComponents.get(startIndex).equals(endComponents.get(endIndex))) {
			lastStartComp = startComponents.get(startIndex);
			lastEndComp = endComponents.get(endIndex);
		} else if (startIndex < endIndex) {
			lastStartComp = null;
			lastEndComp = endComponents.get(endIndex);
		} else if (startIndex > endIndex) {
			lastStartComp = startComponents.get(startIndex);
			lastEndComp = null;
		} else {
			lastStartComp = null;
			lastEndComp = null;
		}

		return Pair.create(lastStartComp, lastEndComp);
	}

	public void validateSliceQueryForRemove() {
		Validator.validateTrue(lastStartComp == null && lastEndComp == null,
				"CQL does not support slice delete with varying compound components");
		Validator.validateFalse(sliceQuery.isLimitSet(), "CQL slice delete does not support LIMIT");
	}
}
