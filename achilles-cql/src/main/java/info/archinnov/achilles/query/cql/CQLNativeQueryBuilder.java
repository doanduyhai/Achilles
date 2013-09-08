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
package info.archinnov.achilles.query.cql;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.operations.CQLNativeQueryMapper;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class CQLNativeQueryBuilder {

	private CQLDaoContext daoContext;
	private String queryString;

	private CQLNativeQueryMapper mapper = new CQLNativeQueryMapper();

	public CQLNativeQueryBuilder(CQLDaoContext daoContext, String queryString) {
		this.daoContext = daoContext;
		this.queryString = queryString;
	}

	/**
	 * Return found rows. The list represents the number of returned rows The
	 * map contains the (column name, column value) of each row. The map is
	 * backed by a LinkedHashMap and thus preserves the columns order as they
	 * were declared in the native query
	 * 
	 * @return List<Map<String, Object>>
	 */
	public List<Map<String, Object>> get() {
		List<Row> rows = daoContext.execute(new SimpleStatement(queryString))
				.all();
		return mapper.mapRows(rows);
	}

	/**
	 * Return the first found row. The map contains the (column name, column
	 * value) of each row. The map is backed by a LinkedHashMap and thus
	 * preserves the columns order as they were declared in the native query
	 * 
	 * @return Map<String, Object>
	 */
	public Map<String, Object> first() {
		List<Row> rows = daoContext.execute(new SimpleStatement(queryString))
				.all();
		List<Map<String, Object>> result = mapper.mapRows(rows);
		if (result.isEmpty())
			return null;
		else
			return result.get(0);
	}
}
