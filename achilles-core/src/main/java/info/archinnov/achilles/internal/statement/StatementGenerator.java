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
package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.collect.FluentIterable;

public class StatementGenerator {

	private static final Logger log = LoggerFactory.getLogger(StatementGenerator.class);

	private SliceQueryStatementGenerator sliceQueryGenerator = new SliceQueryStatementGenerator();

	public RegularStatementWrapper generateSelectSliceQuery(CQLSliceQuery<?> sliceQuery, int limit,int batchSize) {

		log.trace("Generate SELECT statement for slice query");
		EntityMeta meta = sliceQuery.getMeta();

		Select select = generateSelectEntityInternal(meta);
		select = select.limit(limit);
		if (sliceQuery.getCQLOrdering() != null) {
			select.orderBy(sliceQuery.getCQLOrdering());
		}
        select.setFetchSize(batchSize);
		return sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(sliceQuery, select);
	}

	public RegularStatementWrapper generateRemoveSliceQuery(CQLSliceQuery<?> sliceQuery) {

		log.trace("Generate DELETE statement for slice query");

		EntityMeta meta = sliceQuery.getMeta();

		Delete delete = QueryBuilder.delete().from(meta.getTableName());
		return sliceQueryGenerator.generateWhereClauseForDeleteSliceQuery(sliceQuery, delete);
	}

	public RegularStatement generateSelectEntity(EntityMeta entityMeta) {
		final Select select = generateSelectEntityInternal(entityMeta);
		return select;
	}

	protected Select generateSelectEntityInternal(EntityMeta entityMeta) {

		log.trace("Generate SELECT statement for entity class {}", entityMeta.getClassName());

		PropertyMeta idMeta = entityMeta.getIdMeta();

		Selection select = select();

		generateSelectForPrimaryKey(idMeta, select);

		List<PropertyMeta> eagerMetas = FluentIterable.from(entityMeta.getEagerMetas())
				.filter(PropertyType.excludeIdType).toImmutableList();

		for (PropertyMeta pm : eagerMetas) {
			select.column(pm.getPropertyName());
		}
		return select.from(entityMeta.getTableName());
	}

	public Pair<Insert, Object[]> generateInsert(Object entity, EntityMeta entityMeta) {
		PropertyMeta idMeta = entityMeta.getIdMeta();
		Insert insert = insertInto(entityMeta.getTableName());
		final Object[] boundValuesForPK = generateInsertPrimaryKey(entity, idMeta, insert);

		List<PropertyMeta> nonProxyMetas = FluentIterable.from(entityMeta.getAllMetasExceptIdMeta())
				.filter(PropertyType.excludeCounterType).toImmutableList();

		List<PropertyMeta> fieldMetas = new ArrayList<PropertyMeta>(nonProxyMetas);
		//fieldMetas.remove(idMeta);

		final Object[] boundValuesForColumns = new Object[fieldMetas.size()];
		for (int i = 0; i < fieldMetas.size(); i++) {
			PropertyMeta pm = fieldMetas.get(i);
			Object value = pm.getValueFromField(entity);
			value = encodeValueForCassandra(pm, value);
			insert.value(pm.getPropertyName(), value);
			boundValuesForColumns[i] = value;
		}
		final Object[] boundValues = ArrayUtils.addAll(boundValuesForPK, boundValuesForColumns);
		return Pair.create(insert, boundValues);
	}

	public Pair<Update.Where, Object[]> generateUpdateFields(Object entity, EntityMeta entityMeta,
			List<PropertyMeta> pms) {
		log.trace("Generate UPDATE statement for entity class {} and properties {}", entityMeta.getClassName(), pms);
		PropertyMeta idMeta = entityMeta.getIdMeta();
		Update update = update(entityMeta.getTableName());

		Object[] boundValuesForColumns = new Object[pms.size()];
		Assignments assignments = null;
		for (int i = 0; i < pms.size(); i++) {
			PropertyMeta pm = pms.get(i);
			Object value = pm.getValueFromField(entity);
			value = encodeValueForCassandra(pm, value);
			if (i == 0) {
				assignments = update.with(set(pm.getPropertyName(), value));
			} else {
				assignments.and(set(pm.getPropertyName(), value));
			}
			boundValuesForColumns[i] = value;
		}
		final Pair<Update.Where, Object[]> pair = generateWhereClauseForUpdate(entity, idMeta, assignments);

		final Object[] boundValues = ArrayUtils.addAll(boundValuesForColumns, pair.right);
		return Pair.create(pair.left, boundValues);
	}

	private Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta idMeta,
			Assignments update) {
		Update.Where where = null;
		Object[] boundValues;
		Object primaryKey = idMeta.getPrimaryKey(entity);
		if (idMeta.isEmbeddedId()) {
			List<String> componentNames = idMeta.getComponentNames();
			List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey);
			boundValues = new Object[encodedComponents.size()];
			for (int i = 0; i < encodedComponents.size(); i++) {
				String componentName = componentNames.get(i);
				Object componentValue = encodedComponents.get(i);
				if (i == 0) {
					where = update.where(eq(componentName, componentValue));
				} else {
					where.and(eq(componentName, componentValue));
				}
				boundValues[i] = componentValue;
			}
		} else {
			Object id = idMeta.encode(primaryKey);
			where = update.where(eq(idMeta.getPropertyName(), id));
			boundValues = new Object[] { id };
		}
		return Pair.create(where, boundValues);
	}

	private Object[] generateInsertPrimaryKey(Object entity, PropertyMeta idMeta, Insert insert) {
		Object primaryKey = idMeta.getPrimaryKey(entity);
		Object[] boundValues;
		if (idMeta.isEmbeddedId()) {
			List<String> componentNames = idMeta.getComponentNames();
			List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey);
			boundValues = new Object[encodedComponents.size()];
			for (int i = 0; i < encodedComponents.size(); i++) {
				String componentName = componentNames.get(i);
				Object componentValue = encodedComponents.get(i);
				insert.value(componentName, componentValue);
				boundValues[i] = componentValue;
			}
		} else {
			Object id = idMeta.encode(primaryKey);
			insert.value(idMeta.getPropertyName(), id);
			boundValues = new Object[] { id };
		}
		return boundValues;
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

	private void generateSelectForPrimaryKey(PropertyMeta idMeta, Selection select) {
		if (idMeta.isEmbeddedId()) {
			for (String component : idMeta.getComponentNames()) {
				select.column(component);
			}
		} else {
			select.column(idMeta.getPropertyName());
		}
	}

}
