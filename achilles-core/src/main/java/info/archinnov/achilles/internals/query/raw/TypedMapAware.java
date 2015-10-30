/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.query.raw;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import info.archinnov.achilles.type.TypedMap;

public interface TypedMapAware {

    /**
     * Map a list of {@link com.datastax.driver.core.ResultSet} objects to a list
     * of {@link info.archinnov.achilles.type.TypedMap}
     */
    default List<TypedMap> mapResultSetToTypedMaps(ResultSet resultSet) {
        final List<TypedMap> result = new ArrayList<>();
        resultSet
                .iterator()
                .forEachRemaining(row -> result.add(mapRowToTypedMap(row)));
        return result;
    }

    /**
     * Map the {@link com.datastax.driver.core.ResultSet} object to an instance
     * of {@link info.archinnov.achilles.type.TypedMap}
     */
    default TypedMap mapRowToTypedMap(Row row) {
        final TypedMap typedMap = new TypedMap();
        if (row != null) {
            row.getColumnDefinitions()
                    .asList()
                    .forEach(def -> {
                        final String cqlColumn = def.getName();
                        typedMap.put(cqlColumn, row.getObject(cqlColumn));
                    });
        }
        return typedMap;
    }
}
