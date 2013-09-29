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
package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.clustered.ClusteredEntityFactory;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftQueryExecutorImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.iterator.ThriftClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import com.google.common.collect.Lists;

public class ThriftSliceQueryExecutor extends SliceQueryExecutor<ThriftPersistenceContext> {

	private AchillesConsistencyLevelPolicy consistencyPolicy;

	private ClusteredEntityFactory factory = new ClusteredEntityFactory();
	private ThriftQueryExecutorImpl executorImpl = new ThriftQueryExecutorImpl();
	private ThriftPersistenceContextFactory contextFactory;

	public ThriftSliceQueryExecutor(ThriftPersistenceContextFactory contextFactory, ConfigurationContext configContext) {
		super(new ThriftEntityProxifier());
		this.contextFactory = contextFactory;
		this.consistencyPolicy = configContext.getConsistencyPolicy();
		defaultReadLevel = consistencyPolicy.getDefaultGlobalReadConsistencyLevel();
	}

	@Override
	public <T> List<T> get(final SliceQuery<T> sliceQuery) {
		ThriftPersistenceContext context = buildContextForQuery(sliceQuery);
		List<Method> getters = context.isValueless() ? Arrays.<Method> asList() : Arrays.asList(context.getFirstMeta()
				.getGetter());
		PropertyType type = context.isValueless() ? PropertyType.SIMPLE : context.getFirstMeta().type();
		List<T> clusteredEntities = null;
		switch (type) {
		case SIMPLE:
			List<HColumn<Composite, Object>> hColumns = executorImpl.findColumns(sliceQuery, context);
			clusteredEntities = factory.buildClusteredEntities(sliceQuery.getEntityClass(), context, hColumns);
			break;
		case COUNTER:
			List<HCounterColumn<Composite>> hCounterColumns = executorImpl.findCounterColumns(sliceQuery, context);
			clusteredEntities = factory.buildCounterClusteredEntities(sliceQuery.getEntityClass(), context,
					hCounterColumns);
			break;
		default:
			throw new AchillesException("Cannot get entities for clustered value of type '" + type.name()
					+ "' and clustered entity class '" + sliceQuery.getEntityClass().getCanonicalName() + "'");
		}

		return Lists.transform(clusteredEntities, getProxyTransformer(sliceQuery, getters));
	}

	@Override
	public <T> Iterator<T> iterator(final SliceQuery<T> sliceQuery) {
		ThriftPersistenceContext context = buildContextForQuery(sliceQuery);
		PropertyType type = context.isValueless() ? PropertyType.SIMPLE : context.getFirstMeta().type();
		Class<T> entityClass = sliceQuery.getEntityClass();

		switch (type) {
		case SIMPLE:
			ThriftSliceIterator<Object, Object> columnsIterator = executorImpl.getColumnsIterator(sliceQuery, context);
			return new ThriftClusteredEntityIterator<T>(entityClass, columnsIterator, context);
		case COUNTER:
			ThriftCounterSliceIterator<Object> counterColumnsIterator = executorImpl.getCounterColumnsIterator(
					sliceQuery, context);
			return new ThriftCounterClusteredEntityIterator<T>(entityClass, counterColumnsIterator, context);
		default:
			throw new AchillesException("Cannot get iterator for clustered value of type '" + type.name()
					+ "' and clustered entity class '" + entityClass.getCanonicalName() + "'");
		}
	}

	@Override
	public <T> void remove(final SliceQuery<T> sliceQuery) {
		ThriftPersistenceContext context = buildContextForQuery(sliceQuery);
		PropertyType type = context.isValueless() ? PropertyType.SIMPLE : context.getFirstMeta().type();

		if (sliceQuery.hasNoComponent() && sliceQuery.isLimitSet() == false) {
			executorImpl.removeRow(sliceQuery.getPartitionComponents(), context, sliceQuery.getConsistencyLevel());
		} else {
			switch (type) {
			case SIMPLE:
				List<HColumn<Composite, Object>> hColumns = executorImpl.findColumns(sliceQuery, context);
				executorImpl.removeColumns(hColumns, sliceQuery.getConsistencyLevel(), context);
				break;
			case COUNTER:
				List<HCounterColumn<Composite>> hCounterColumns = executorImpl.findCounterColumns(sliceQuery, context);
				executorImpl.removeCounterColumns(hCounterColumns, sliceQuery.getConsistencyLevel(), context);
				break;
			default:
				throw new AchillesException("Cannot remove clustered value of type '" + type.name()
						+ "' and clustered entity class '" + sliceQuery.getEntityClass().getCanonicalName() + "'");
			}
		}
	}

	@Override
	protected <T> ThriftPersistenceContext buildContextForQuery(SliceQuery<T> sliceQuery) {
		ConsistencyLevel cl = sliceQuery.getConsistencyLevel() == null ? defaultReadLevel : sliceQuery
				.getConsistencyLevel();
		return contextFactory.newContextForSliceQuery(sliceQuery.getEntityClass(), sliceQuery.getPartitionComponents(),
				cl);
	}

	@Override
	protected <T> ThriftPersistenceContext buildNewContext(final SliceQuery<T> sliceQuery, T clusteredEntity) {
		return contextFactory.newContext(clusteredEntity);
	}
}
