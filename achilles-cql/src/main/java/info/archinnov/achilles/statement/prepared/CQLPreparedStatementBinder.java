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

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.FluentIterable;

public class CQLPreparedStatementBinder {

	public BoundStatementWrapper bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity) {
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

		Object[] boundValues = new Object[values.size()];
		BoundStatement bs = ps.bind(values.toArray(boundValues));

		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindForUpdate(PreparedStatement ps, EntityMeta entityMeta, List<PropertyMeta> pms,
			Object entity) {
		List<Object> values = new ArrayList<Object>();
		for (PropertyMeta pm : pms) {
			Object value = pm.getValueFromField(entity);
			value = encodeValueForCassandra(pm, value);
			values.add(value);
		}
		Object primaryKey = entityMeta.getPrimaryKey(entity);
		values.addAll(bindPrimaryKey(primaryKey, entityMeta.getIdMeta()));

		Object[] boundValues = new Object[values.size()];
		BoundStatement bs = ps.bind(values.toArray(boundValues));

		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindStatementWithOnlyPKInWhereClause(PreparedStatement ps, EntityMeta entityMeta,
			Object primaryKey) {
		PropertyMeta idMeta = entityMeta.getIdMeta();
		List<Object> values = bindPrimaryKey(primaryKey, idMeta);

		Object[] boundValues = new Object[values.size()];
		BoundStatement bs = ps.bind(values.toArray(boundValues));

		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindForSimpleCounterIncrementDecrement(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey, Long increment) {
		Object[] boundValues = ArrayUtils.add(extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey), 0,
				increment);

		BoundStatement bs = ps.bind(boundValues);

		return new BoundStatementWrapper(bs, boundValues);

	}

	public BoundStatementWrapper bindForSimpleCounterSelect(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey) {
		Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindForSimpleCounterDelete(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey) {
		Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
		BoundStatement bs = ps.bind(boundValues);
		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindForClusteredCounterIncrementDecrement(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey, Long increment) {
		List<Object> primarykeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] keys = ArrayUtils.add(primarykeys.toArray(new Object[primarykeys.size()]), 0, increment);

		BoundStatement bs = ps.bind(keys);

		return new BoundStatementWrapper(bs, keys);
	}

	public BoundStatementWrapper bindForClusteredCounterSelect(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey) {
		List<Object> primarykeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] boundValues = primarykeys.toArray(new Object[primarykeys.size()]);

		BoundStatement bs = ps.bind(boundValues);

		return new BoundStatementWrapper(bs, boundValues);
	}

	public BoundStatementWrapper bindForClusteredCounterDelete(PreparedStatement ps, EntityMeta entityMeta,
			PropertyMeta pm, Object primaryKey) {
		List<Object> primarykeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta());
		Object[] boundValues = primarykeys.toArray(new Object[primarykeys.size()]);
		BoundStatement bs = ps.bind(boundValues);

		return new BoundStatementWrapper(bs, boundValues);
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
