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
package info.archinnov.achilles.statement.prepared;

import static info.archinnov.achilles.consistency.ConsistencyConvertor.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.FluentIterable;

public class PreparedStatementBinder {

	private static final Logger log = LoggerFactory.getLogger(PreparedStatementBinder.class);

	public BoundStatementWrapper bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity,
			ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for insert of entity {}", ps.getQueryString(), entity);
		List<Object> values = new ArrayList<Object>();
		Object primaryKey = entityMeta.getPrimaryKey(entity);
		values.addAll(bindPrimaryKey(primaryKey, entityMeta.getIdMeta()));

		List<PropertyMeta> nonProxyMetas = FluentIterable.from(entityMeta.getAllMetasExceptIdMeta())
				.filter(PropertyType.excludeCounterType).toImmutableList();

		List<PropertyMeta> fieldMetas = new ArrayList<PropertyMeta>(nonProxyMetas);

		for (PropertyMeta pm : fieldMetas) {
			Object value = pm.getValueFromField(entity);
			value = encodeValueForCassandra(pm, value);
			values.add(value);
		}

		BoundStatement bs = ps.bind(values.toArray());
		return new BoundStatementWrapper(bs, values.toArray(), getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForUpdate(PreparedStatement ps, EntityMeta entityMeta, List<PropertyMeta> pms,
			Object entity, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for properties {} update of entity {}", ps.getQueryString(), pms, entity);
		List<Object> values = new ArrayList<Object>();
		for (PropertyMeta pm : pms) {
			Object value = pm.getValueFromField(entity);
			value = encodeValueForCassandra(pm, value);
			values.add(value);
		}
		Object primaryKey = entityMeta.getPrimaryKey(entity);
		values.addAll(bindPrimaryKey(primaryKey, entityMeta.getIdMeta()));
		BoundStatement bs = ps.bind(values.toArray());

		return new BoundStatementWrapper(bs, values.toArray(), getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindStatementWithOnlyPKInWhereClause(PreparedStatement ps, EntityMeta entityMeta,
			Object primaryKey, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} with primary key {}", ps.getQueryString(), primaryKey);
		PropertyMeta idMeta = entityMeta.getIdMeta();
		List<Object> values = bindPrimaryKey(primaryKey, idMeta);

		BoundStatement bs = ps.bind(values.toArray());
		return new BoundStatementWrapper(bs, values.toArray(), getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForSimpleCounterIncrementDecrement(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey, Long increment, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for simple counter increment of {} using primary key {} and value {}",
				ps.getQueryString(), pm, primaryKey, increment);
		Object[] boundValues = ArrayUtils.add(extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey), 0,
				increment);

		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues, getCQLLevel(consistencyLevel));

	}

	public BoundStatementWrapper bindForSimpleCounterSelect(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for simple counter read of {} using primary key {}", ps.getQueryString(),
				pm, primaryKey);
		Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues, getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForSimpleCounterDelete(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for simple counter delete for {} using primary key {}",
				ps.getQueryString(), pm, primaryKey);
		Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues, getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForClusteredCounterIncrementDecrement(PreparedStatement ps, EntityMeta entityMeta,
			Object primaryKey, Long increment, ConsistencyLevel consistencyLevel) {
		log.trace(
				"Bind prepared statement {} for clustered counter increment/decrement for {} using primary key {} and value {}",
				ps.getQueryString(), entityMeta, primaryKey, increment);

		List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] keys = ArrayUtils.add(primaryKeys.toArray(new Object[primaryKeys.size()]), 0, increment);

		BoundStatement bs = ps.bind(keys);

		return new BoundStatementWrapper(bs, keys, getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForClusteredCounterSelect(PreparedStatement ps, EntityMeta entityMeta,
			Object primaryKey, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for clustered counter read for {} using primary key {}",
				ps.getQueryString(), entityMeta, primaryKey);
		List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] boundValues = primaryKeys.toArray(new Object[primaryKeys.size()]);

		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues, getCQLLevel(consistencyLevel));
	}

	public BoundStatementWrapper bindForClusteredCounterDelete(PreparedStatement ps, EntityMeta entityMeta,
			Object primaryKey, ConsistencyLevel consistencyLevel) {
		log.trace("Bind prepared statement {} for simple counter delete for {} using primary key {}",
				ps.getQueryString(), entityMeta, primaryKey);
		List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] boundValues = primaryKeys.toArray(new Object[primaryKeys.size()]);
		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues, getCQLLevel(consistencyLevel));
	}

	private List<Object> bindPrimaryKey(Object primaryKey, PropertyMeta idMeta) {
		List<Object> values = new ArrayList<Object>();
		if (idMeta.isEmbeddedId()) {
			values.addAll(idMeta.encodeToComponents(primaryKey));
		} else {
			values.add(idMeta.encode(primaryKey));
		}
		return values;
	}

	private Object encodeValueForCassandra(PropertyMeta pm, Object value) {
		if (value != null) {
			switch (pm.type()) {
			case SIMPLE:
			case LAZY_SIMPLE:
				return pm.encode(value);
			case LIST:
			case LAZY_LIST:
				return pm.encode((List<?>) value);
			case SET:
			case LAZY_SET:
				return pm.encode((Set<?>) value);
			case MAP:
			case LAZY_MAP:
				return pm.encode((Map<?, ?>) value);
			default:
				throw new AchillesException("Cannot encode value '" + value + "' for Cassandra for property '"
						+ pm.getPropertyName() + "' of type '" + pm.type().name() + "'");
			}
		}
		return value;
	}

	private Object[] extractValuesForSimpleCounterBinding(EntityMeta entityMeta, PropertyMeta pm, Object primaryKey) {
		PropertyMeta idMeta = entityMeta.getIdMeta();
		String fqcn = entityMeta.getClassName();
		String primaryKeyAsString = idMeta.forceEncodeToJSON(primaryKey);
		String propertyName = pm.getPropertyName();

		return new Object[] { fqcn, primaryKeyAsString, propertyName };
	}
}
