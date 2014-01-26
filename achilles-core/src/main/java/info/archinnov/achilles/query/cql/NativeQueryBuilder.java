/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.query.cql;

import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.statement.wrapper.SimpleStatementWrapper;
import info.archinnov.achilles.type.TypedMap;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class NativeQueryBuilder {
	private static final Logger log = LoggerFactory.getLogger(NativeQueryBuilder.class);

	private DaoContext daoContext;
	private String queryString;

	private NativeQueryMapper mapper = new NativeQueryMapper();

	private Object[] boundValues;

	public NativeQueryBuilder(DaoContext daoContext, String queryString, Object... boundValues) {
		this.daoContext = daoContext;
		this.queryString = queryString;
		this.boundValues = boundValues;
	}

	/**
	 * Return found rows. The list represents the number of returned rows The
	 * map contains the (column name, column value) of each row. The map is
	 * backed by a LinkedHashMap and thus preserves the columns order as they
	 * were declared in the native query
	 * 
	 * @return List<TypedMap>
	 */
	public List<TypedMap> get() {
		log.debug("Get results for native query {}", queryString);
		List<Row> rows = daoContext.execute(new SimpleStatementWrapper(queryString, boundValues)).all();
		return mapper.mapRows(rows);
	}

	/**
	 * Return the first found row. The map contains the (column name, column
	 * value) of each row. The map is backed by a LinkedHashMap and thus
	 * preserves the columns order as they were declared in the native query
	 * 
	 * @return TypedMap
	 */
	public TypedMap first() {
		log.debug("Get first result for native query {}", queryString);
		List<Row> rows = daoContext.execute(new SimpleStatementWrapper(queryString, boundValues)).all();
		List<TypedMap> result = mapper.mapRows(rows);
		if (result.isEmpty())
			return null;
		else
			return result.get(0);
	}

	/**
	 * Execute statement without returning result. Useful for
	 * INSERT/UPDATE/DELETE and DDL statements
	 */
	public void execute() {
		log.debug("Execute native query {}", queryString);
		daoContext.execute(new SimpleStatementWrapper(queryString, boundValues));
	}
}
