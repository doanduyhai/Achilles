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

import info.archinnov.achilles.type.TypedMap;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

public class NativeQueryMapper {

	private static final Logger log = LoggerFactory.getLogger(NativeQueryMapper.class);

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
		final TypedMap typedMap = new TypedMap();
		if (row != null) {
			for (ColumnDefinitions.Definition def : row.getColumnDefinitions().asList()) {
				final String cqlColumn = def.getName();
				typedMap.put(cqlColumn, row.getObject(cqlColumn));
			}
		}
		return typedMap;
	}

    public enum Singleton {
        INSTANCE;

        private final NativeQueryMapper instance = new NativeQueryMapper();

        public NativeQueryMapper get() {
            return instance;
        }
    }
}
