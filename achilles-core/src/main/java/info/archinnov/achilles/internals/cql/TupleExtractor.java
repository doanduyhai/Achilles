/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.cql;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;

import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.MapProperty;

public class TupleExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleExtractor.class);

    public static Object extractType(TupleValue tupleValue, DataType dataType, AbstractProperty<?, ?, ?> nestedProperty, int position) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extracting data from tuple value for data type %s and property meta %s",
                    dataType, nestedProperty));
        }

        switch (dataType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return tupleValue.getString(position);
            case BIGINT:
                return tupleValue.getLong(position);
            case BLOB:
                return tupleValue.getBytes(position);
            case COUNTER:
                return tupleValue.getLong(position);
            case DECIMAL:
                return tupleValue.getDecimal(position);
            case DOUBLE:
                return tupleValue.getDouble(position);
            case FLOAT:
                return tupleValue.getFloat(position);
            case INET:
                return tupleValue.getInet(position);
            case INT:
                return tupleValue.getInt(position);
            case VARINT:
                return tupleValue.getVarint(position);
            case SMALLINT:
                return tupleValue.getShort(position);
            case TINYINT:
                return tupleValue.getByte(position);
            case TIMESTAMP:
                return tupleValue.getTimestamp(position);
            case DATE:
                return tupleValue.getDate(position);
            case TIME:
                return tupleValue.getLong(position);
            case UUID:
            case TIMEUUID:
                return tupleValue.getUUID(position);
            case LIST:
            case SET:
                return tupleValue.get(position, nestedProperty.valueToTypeToken);
            case MAP:
                final MapProperty<?, ?, ?, ?, ?> mapProperty = (MapProperty<?, ?, ?, ?, ?>) nestedProperty;
                return tupleValue.getMap(position, mapProperty.keyProperty.valueToTypeToken, mapProperty.valueProperty.valueToTypeToken);
            case UDT:
                return tupleValue.getUDTValue(position);
            case TUPLE:
                return tupleValue.getTupleValue(position);
            default:
                return tupleValue.getBytesUnsafe(position);
        }
    }
}
