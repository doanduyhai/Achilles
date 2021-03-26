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

import static com.datastax.driver.core.DataType.Name.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;


public class TypeMapper {

    private static final Map<Name, Class<?>> cql2Java = new HashMap<>();

    static {
        cql2Java.put(ASCII, String.class);
        cql2Java.put(BIGINT, Long.class);
        cql2Java.put(BLOB, ByteBuffer.class);
        cql2Java.put(BOOLEAN, Boolean.class);
        cql2Java.put(COUNTER, Long.class);
        cql2Java.put(DECIMAL, BigDecimal.class);
        cql2Java.put(DOUBLE, Double.class);
        cql2Java.put(FLOAT, Float.class);
        cql2Java.put(INET, InetAddress.class);
        cql2Java.put(INT, Integer.class);
        cql2Java.put(SMALLINT, Short.class);
        cql2Java.put(TINYINT, Byte.class);
        cql2Java.put(DATE, LocalDate.class);
        cql2Java.put(TIME, Long.class);
        cql2Java.put(TEXT, String.class);
        cql2Java.put(TIMESTAMP, Date.class);
        cql2Java.put(UUID, UUID.class);
        cql2Java.put(VARCHAR, String.class);
        cql2Java.put(VARINT, BigInteger.class);
        cql2Java.put(TIMEUUID, UUID.class);
        cql2Java.put(LIST, List.class);
        cql2Java.put(SET, Set.class);
        cql2Java.put(MAP, Map.class);
        cql2Java.put(CUSTOM, ByteBuffer.class);
        cql2Java.put(UDT, UDTValue.class);
        cql2Java.put(TUPLE, TupleValue.class);
        cql2Java.put(DURATION, Duration.class);
    }

    public static Class<?> toJavaType(Name cqlType) {
        return cql2Java.get(cqlType);
    }
}
