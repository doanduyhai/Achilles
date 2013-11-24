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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.collect.FluentIterable;

public class PreparedStatementGenerator {
	private static final Logger log = LoggerFactory.getLogger(PreparedStatementGenerator.class);

	public PreparedStatement prepareInsertPS(Session session, EntityMeta entityMeta) {
		log.trace("Generate prepared statement for INSERT on {}", entityMeta);
		PropertyMeta idMeta = entityMeta.getIdMeta();
		Insert insert = insertInto(entityMeta.getTableName());
		prepareInsertPrimaryKey(idMeta, insert);

		List<PropertyMeta> nonProxyMetas = FluentIterable.from(entityMeta.getAllMetasExceptIdMeta())
				.filter(PropertyType.excludeCounterType).toImmutableList();

		List<PropertyMeta> fieldMetas = new ArrayList<PropertyMeta>(nonProxyMetas);
		fieldMetas.remove(idMeta);

		for (PropertyMeta pm : fieldMetas) {
			insert.value(pm.getPropertyName(), bindMarker());
		}
		return session.prepare(insert.getQueryString());
	}

	public PreparedStatement prepareSelectFieldPS(Session session, EntityMeta entityMeta, PropertyMeta pm) {
		log.trace("Generate prepared statement for SELECT property {}", pm);

		PropertyMeta idMeta = entityMeta.getIdMeta();

		if (pm.isCounter()) {
			throw new IllegalArgumentException("Cannot prepare statement for property '" + pm.getPropertyName()
					+ "' of entity '" + entityMeta.getClassName() + "' because it is a counter type");
		} else {
			Selection select = prepareSelectField(pm, select());
			Select from = select.from(entityMeta.getTableName());
			RegularStatement statement = prepareWhereClauseForSelect(idMeta, from);
			return session.prepare(statement.getQueryString());
		}
	}

	public PreparedStatement prepareUpdateFields(Session session, EntityMeta entityMeta, List<PropertyMeta> pms) {

		log.trace("Generate prepared statement for UPDATE properties {}", pms);

		PropertyMeta idMeta = entityMeta.getIdMeta();
		Update update = update(entityMeta.getTableName());

		Assignments assignments = null;
		for (int i = 0; i < pms.size(); i++) {
			PropertyMeta pm = pms.get(i);
			if (i == 0) {
				assignments = update.with(set(pm.getPropertyName(), bindMarker()));
			} else {
				assignments.and(set(pm.getPropertyName(), bindMarker()));
			}
		}
		RegularStatement statement = prepareWhereClauseForUpdate(idMeta, assignments);
		return session.prepare(statement.getQueryString());
	}

	public PreparedStatement prepareSelectEagerPS(Session session, EntityMeta entityMeta) {
		log.trace("Generate prepared statement for SELECT of {}", entityMeta);

		PropertyMeta idMeta = entityMeta.getIdMeta();

		Selection select = select();

		for (PropertyMeta pm : entityMeta.getEagerMetas()) {
			select = prepareSelectField(pm, select);
		}
		Select from = select.from(entityMeta.getTableName());

		RegularStatement statement = prepareWhereClauseForSelect(idMeta, from);
		return session.prepare(statement.getQueryString());
	}

	public Map<CQLQueryType, PreparedStatement> prepareSimpleCounterQueryMap(Session session) {

		StringBuilder incr = new StringBuilder();
		incr.append("UPDATE ").append(CQL_COUNTER_TABLE).append(" ");
		incr.append("SET ").append(CQL_COUNTER_VALUE).append(" = ");
		incr.append(CQL_COUNTER_VALUE).append(" + ? ");
		incr.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		incr.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		incr.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder decr = new StringBuilder();
		decr.append("UPDATE ").append(CQL_COUNTER_TABLE).append(" ");
		decr.append("SET ").append(CQL_COUNTER_VALUE).append(" = ");
		decr.append(CQL_COUNTER_VALUE).append(" - ? ");
		decr.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		decr.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		decr.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder select = new StringBuilder();
		select.append("SELECT ").append(CQL_COUNTER_VALUE).append(" ");
		select.append("FROM ").append(CQL_COUNTER_TABLE).append(" ");
		select.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		select.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		select.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder delete = new StringBuilder();
		delete.append("DELETE FROM ").append(CQL_COUNTER_TABLE).append(" ");
		delete.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		delete.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		delete.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		Map<CQLQueryType, PreparedStatement> counterPSMap = new HashMap<AchillesCounter.CQLQueryType, PreparedStatement>();
		counterPSMap.put(INCR, session.prepare(incr.toString()));
		counterPSMap.put(DECR, session.prepare(decr.toString()));
		counterPSMap.put(SELECT, session.prepare(select.toString()));
		counterPSMap.put(DELETE, session.prepare(delete.toString()));

		return counterPSMap;
	}

	public Map<CQLQueryType, PreparedStatement> prepareClusteredCounterQueryMap(Session session, EntityMeta meta) {
		PropertyMeta idMeta = meta.getIdMeta();
		PropertyMeta counterMeta = meta.getFirstMeta();
		String tableName = meta.getTableName();
		String counterName = counterMeta.getPropertyName();

		RegularStatement incrStatement = prepareWhereClauseForUpdate(idMeta,
				update(tableName).with(incr(counterName, 100L)));
		String incr = incrStatement.getQueryString().replaceAll("100", "?");

		RegularStatement decrStatement = prepareWhereClauseForUpdate(idMeta,
				update(tableName).with(decr(counterName, 100L)));
		String decr = decrStatement.getQueryString().replaceAll("100", "?");

		RegularStatement selectStatement = prepareWhereClauseForSelect(idMeta, select(counterName).from(tableName));
		String select = selectStatement.getQueryString();

		RegularStatement deleteStatement = prepareWhereClauseForDelete(idMeta, QueryBuilder.delete().from(tableName));
		String delete = deleteStatement.getQueryString();

		Map<CQLQueryType, PreparedStatement> clusteredCounterPSMap = new HashMap<AchillesCounter.CQLQueryType, PreparedStatement>();
		clusteredCounterPSMap.put(INCR, session.prepare(incr.toString()));
		clusteredCounterPSMap.put(DECR, session.prepare(decr.toString()));
		clusteredCounterPSMap.put(SELECT, session.prepare(select.toString()));
		clusteredCounterPSMap.put(DELETE, session.prepare(delete.toString()));

		return clusteredCounterPSMap;
	}

	private Selection prepareSelectField(PropertyMeta pm, Selection select) {
		if (pm.isEmbeddedId()) {
			for (String component : pm.getComponentNames()) {
				select = select.column(component);
			}
		} else {
			select = select.column(pm.getPropertyName());
		}
		return select;
	}

	private void prepareInsertPrimaryKey(PropertyMeta idMeta, Insert insert) {
		if (idMeta.isEmbeddedId()) {
			for (String component : idMeta.getComponentNames()) {
				insert.value(component, bindMarker());
			}
		} else {
			insert.value(idMeta.getPropertyName(), bindMarker());
		}
	}

	private RegularStatement prepareWhereClauseForSelect(PropertyMeta idMeta, Select from) {
		RegularStatement statement;
		if (idMeta.isEmbeddedId()) {
			Select.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getComponentNames()) {
				if (i == 0) {
					where = from.where(eq(clusteredId, bindMarker()));
				} else {
					where.and(eq(clusteredId, bindMarker()));
				}
				i++;
			}
			statement = where;
		} else {
			statement = from.where(eq(idMeta.getPropertyName(), bindMarker()));
		}
		return statement;
	}

	private RegularStatement prepareWhereClauseForUpdate(PropertyMeta idMeta, Assignments update) {
		RegularStatement statement;
		if (idMeta.isEmbeddedId()) {
			Update.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getComponentNames()) {
				if (i == 0) {
					where = update.where(eq(clusteredId, bindMarker()));
				} else {
					where.and(eq(clusteredId, bindMarker()));
				}
				i++;
			}
			statement = where;
		} else {
			statement = update.where(eq(idMeta.getPropertyName(), bindMarker()));
		}
		return statement;
	}

	public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta entityMeta) {

		log.trace("Generate prepared statement for DELETE of {}", entityMeta);

		PropertyMeta idMeta = entityMeta.getIdMeta();

		Map<String, PreparedStatement> removePSs = new HashMap<String, PreparedStatement>();

		Delete mainFrom = QueryBuilder.delete().from(entityMeta.getTableName());
		RegularStatement mainStatement = prepareWhereClauseForDelete(idMeta, mainFrom);
		removePSs.put(entityMeta.getTableName(), session.prepare(mainStatement.getQueryString()));

		return removePSs;
	}

	private RegularStatement prepareWhereClauseForDelete(PropertyMeta idMeta, Delete mainFrom) {
		RegularStatement mainStatement;
		if (idMeta.isEmbeddedId()) {
			Delete.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getComponentNames()) {
				if (i == 0) {
					where = mainFrom.where(eq(clusteredId, bindMarker()));
				} else {
					where.and(eq(clusteredId, bindMarker()));
				}
				i++;
			}
			mainStatement = where;
		} else {
			mainStatement = mainFrom.where(eq(idMeta.getPropertyName(), bindMarker()));
		}
		return mainStatement;
	}
}
