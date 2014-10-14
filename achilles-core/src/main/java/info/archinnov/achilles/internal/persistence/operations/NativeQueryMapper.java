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
package info.archinnov.achilles.internal.persistence.operations;

import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.TypedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class NativeQueryMapper {

	private static final Logger log = LoggerFactory.getLogger(NativeQueryMapper.class);

    private RowMethodInvoker cqlRowInvoker = RowMethodInvoker.Singleton.INSTANCE.get();

	public List<TypedMap> mapRows(List<Row> rows) {
		log.trace("Map CQL rows to List<Map<ColumnName,Value>>");
		List<TypedMap> result = new ArrayList<>();
		if (!rows.isEmpty()) {
			for (Row row : rows) {
                result.add(mapRow(row));
			}
		}
		return result;
	}

    public TypedMap mapRow(Row row) {
		log.trace("Map CQL row to a map of <ColumnName,Value>");
		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        Validator.validateNotNull(columnDefinitions,"Impossible to fetch column definitions for the row '%s'", row);
        TypedMap line = new TypedMap();
        for (Definition column : columnDefinitions) {
            line.put(column.getName(), mapColumn(row, column));
        }
        return line;
    }

	private Object mapColumn(Row row, Definition column) {
		if (log.isTraceEnabled()) {
			log.trace("Extract data from CQL column [keyspace:{},table:{},column:{}]", column.getKeyspace(),
					column.getTable(), column.getName());
		}

		DataType type = column.getType();
		Class<?> javaClass = type.asJavaClass();
		String name = column.getName();
		Object value;
		if (type.isCollection()) {
			List<DataType> typeArguments = type.getTypeArguments();
			if (List.class.isAssignableFrom(javaClass)) {
				value = row.getList(name, typeArguments.get(0).asJavaClass());
			} else if (Set.class.isAssignableFrom(javaClass)) {
				value = row.getSet(name, typeArguments.get(0).asJavaClass());
			} else {
				value = row.getMap(name, typeArguments.get(0).asJavaClass(), typeArguments.get(1).asJavaClass());
			}
		} else {
			value = cqlRowInvoker.invokeOnRowForType(row, javaClass, name);
		}
        return value;
	}

    public static enum Singleton {
        INSTANCE;

        private final NativeQueryMapper instance = new NativeQueryMapper();

        public NativeQueryMapper get() {
            return instance;
        }
    }
}
