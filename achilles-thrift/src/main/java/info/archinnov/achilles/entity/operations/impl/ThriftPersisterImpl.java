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
package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.CounterBuilder.CounterImpl;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;

public class ThriftPersisterImpl {
	private static final Logger log = LoggerFactory.getLogger(ThriftPersisterImpl.class);
	private static final String EMPTY = "";

	private ThriftCompositeFactory compositeFactory = new ThriftCompositeFactory();

	public void batchPersistSimpleProperty(ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		Composite name = compositeFactory.createForBatchInsertSingleValue(propertyMeta);
		String value = propertyMeta.forceEncodeToJSON(propertyMeta.getValueFromField(context.getEntity()));
		if (value != null) {
			if (log.isTraceEnabled()) {
				log.trace(
						"Batch persisting simple property {} from entity of class {} and primary key {} with column name {}",
						propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
						context.getPrimaryKey(), format(name));
			}
			Object rowKey = buildRowKey(context);
			context.getEntityDao().insertColumnBatch(rowKey, name, value, context.getTtt(), context.getTimestamp(),
					context.getEntityMutator(context.getTableName()));
		}
	}

	public void persistCounter(ThriftPersistenceContext context, PropertyMeta propertyMeta) {

		Object counter = propertyMeta.getValueFromField(context.getEntity());
		String entityClassName = context.getEntityClass().getCanonicalName();
		if (counter != null) {

			PropertyMeta idMeta = context.getEntityMeta().getIdMeta();
			Object primaryKey = context.getPrimaryKey();
			Composite rowKey = compositeFactory.createRowKeyForCounter(propertyMeta.fqcn(), primaryKey, idMeta);
			Composite name = compositeFactory.createBaseForCounterGet(propertyMeta);
			if (log.isTraceEnabled()) {
				log.trace(
						"Batch persisting counter property {} from entity of class {} and primary key {} with column name {}",
						propertyMeta.getPropertyName(), entityClassName, context.getPrimaryKey(), format(name));
			}

			Validator.validateTrue(CounterImpl.class.isAssignableFrom(counter.getClass()),
					"Counter clustered entity '%s' value should be of type '%s'", entityClassName,
					CounterImpl.class.getCanonicalName());

			CounterImpl counterValue = (CounterImpl) counter;
			context.getCounterDao().incrementCounter(rowKey, name, counterValue.get());
		}
	}

	public <V> void batchPersistList(List<V> list, ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		int count = 0;
		for (V value : list) {
			String stringValue = propertyMeta.forceEncodeToJSON(value);
			if (stringValue != null) {
				Composite name = compositeFactory.createForBatchInsertList(propertyMeta, count);
				if (log.isTraceEnabled()) {
					log.trace(
							"Batch persisting list property {} from entity of class {} and primary key {} with column name {}",
							propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
							context.getPrimaryKey(), format(name));
				}
				Object rowKey = buildRowKey(context);
				context.getEntityDao().insertColumnBatch(rowKey, name, stringValue, context.getTtt(),
						context.getTimestamp(), context.getEntityMutator(context.getTableName()));
			}
			count++;
		}
	}

	public <V> void batchPersistSet(Set<V> set, ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		for (V value : set) {

			String valueAsString = propertyMeta.forceEncodeToJSON(value);
			if (valueAsString != null) {
				Composite name = compositeFactory.createForBatchInsertSetOrMap(propertyMeta, valueAsString);
				if (log.isTraceEnabled()) {
					log.trace(
							"Batch persisting set property {} from entity of class {} and primary key {} with column name {}",
							propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
							context.getPrimaryKey(), format(name));
				}
				Object rowKey = buildRowKey(context);
				context.getEntityDao().insertColumnBatch(rowKey, name, EMPTY, context.getTtt(), context.getTimestamp(),
						context.getEntityMutator(context.getTableName()));
			}
		}
	}

	public void batchPersistMap(Map<?, ?> map, ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		for (Entry<?, ?> entry : map.entrySet()) {

			String keyAsString = propertyMeta.forceEncodeToJSON(entry.getKey());
			String valueAsString = propertyMeta.forceEncodeToJSON(entry.getValue());

			Composite name = compositeFactory.createForBatchInsertSetOrMap(propertyMeta, keyAsString);

			if (log.isTraceEnabled()) {
				log.trace(
						"Batch persisting map property {} from entity of class {} and primary key {} with column name {}",
						propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
						context.getPrimaryKey(), format(name));
			}
			Object rowKey = buildRowKey(context);
			context.getEntityDao().insertColumnBatch(rowKey, name, valueAsString, context.getTtt(),
					context.getTimestamp(), context.getEntityMutator(context.getTableName()));
		}
	}

	public void persistClusteredEntity(ThriftPersistenceContext context, Object clusteredValue) {
		Object rowKey = buildRowKey(context);

		String tableName = context.getTableName();
		String className = context.getEntityClass().getCanonicalName();

		Composite comp = compositeFactory.createCompositeForClusteringComponents(context);

		ThriftGenericWideRowDao dao = context.findWideRowDao(tableName);
		Mutator<Object> mutator = context.getWideRowMutator(tableName);

		if (context.isValueless()) {
			dao.setValueBatch(rowKey, comp, "", context.getTtt(), context.getTimestamp(), mutator);
		} else {
			PropertyMeta pm = context.getFirstMeta();
			if (pm.isCounter()) {
				Validator.validateTrue(CounterImpl.class.isAssignableFrom(clusteredValue.getClass()),
						"Counter clustered entity '%s' value should be of type '%s'", className,
						CounterImpl.class.getCanonicalName());
				CounterImpl counterValue = (CounterImpl) clusteredValue;
				dao.incrementCounter(rowKey, comp, counterValue.get());
			} else {
				Object persistentValue = pm.encode(clusteredValue);
				dao.setValueBatch(rowKey, comp, persistentValue, context.getTtt(), context.getTimestamp(), mutator);
			}
		}
	}

	public void persistClusteredValueBatch(ThriftPersistenceContext context, Object clusteredValue) {
		Object rowKey = buildRowKey(context);

		EntityMeta meta = context.getEntityMeta();
		PropertyMeta pm = meta.getFirstMeta();
		String tableName = meta.getTableName();
		Composite comp = compositeFactory.createCompositeForClusteringComponents(context);

		ThriftGenericWideRowDao dao = context.findWideRowDao(tableName);
		Mutator<Object> mutator = context.getWideRowMutator(tableName);
		Object persistentValue = pm.encode(clusteredValue);
		dao.setValueBatch(rowKey, comp, persistentValue, context.getTtt(), context.getTimestamp(), mutator);
	}

	public void removeEntityBatch(ThriftPersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();
		Object rowKey = buildRowKey(context);

		log.trace("Batch removing wide row of class {} and primary key {}",
				context.getEntityClass().getCanonicalName(), context.getPrimaryKey());
		Mutator<Object> entityMutator = context.getEntityMutator(entityMeta.getTableName());
		context.getEntityDao().removeRowBatch(rowKey, entityMutator);
	}

	public void remove(ThriftPersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();
		Object rowKey = buildRowKey(context);

		log.trace("Batch removing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
				context.getPrimaryKey());

		Mutator<Object> entityMutator = context.getEntityMutator(entityMeta.getTableName());
		context.getEntityDao().removeRowBatch(rowKey, entityMutator);

		List<PropertyMeta> pms = FluentIterable.from(entityMeta.getAllMetasExceptIdMeta()).toImmutableList();

		for (PropertyMeta propertyMeta : pms) {
			if (propertyMeta.isCounter()) {
				removeSimpleCounter(context, propertyMeta);
			}
		}
	}

	public void removeClusteredEntity(ThriftPersistenceContext context) {
		Object rowKey = buildRowKey(context);

		boolean isCounter = context.isValueless() ? false : context.getFirstMeta().isCounter();

		String tableName = context.getTableName();

		Composite comp = compositeFactory.createCompositeForClusteringComponents(context);

		ThriftGenericWideRowDao dao = context.findWideRowDao(tableName);
		Mutator<Object> mutator = context.getWideRowMutator(tableName);

		if (isCounter) {
			dao.removeCounterBatch(rowKey, comp, mutator);
		} else {
			dao.removeColumnBatch(rowKey, comp, mutator);
		}

	}

	public void removePropertyBatch(ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		Composite start = compositeFactory.createBaseForQuery(propertyMeta, ComponentEquality.EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);

		if (log.isTraceEnabled()) {
			log.trace("Batch removing property {} of class {} and primary key {} with column names {}  / {}",
					propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
					context.getPrimaryKey(), format(start), format(end));
		}
		context.getEntityDao().removeColumnRangeBatch(context.getPrimaryKey(), start, end,
				context.getEntityMutator(context.getTableName()));
	}

	private void removeSimpleCounter(ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		Composite keyComp = compositeFactory.createRowKeyForCounter(propertyMeta.fqcn(), context.getPrimaryKey(),
				propertyMeta.counterIdMeta());
		Composite com = compositeFactory.createForBatchInsertSingleCounter(propertyMeta);

		log.trace("Batch removing counter property {} of class {} and primary key {}", propertyMeta.getPropertyName(),
				context.getEntityClass().getCanonicalName(), context.getPrimaryKey());

		context.getCounterDao().removeCounterBatch(keyComp, com, context.getCounterMutator());
	}

	private Object buildRowKey(ThriftPersistenceContext context) {
		return compositeFactory.buildRowKey(context);
	}
}
