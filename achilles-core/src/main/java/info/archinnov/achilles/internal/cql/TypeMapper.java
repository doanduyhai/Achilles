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
package info.archinnov.achilles.internal.cql;

import static com.datastax.driver.core.DataType.Name.ASCII;
import static com.datastax.driver.core.DataType.Name.BIGINT;
import static com.datastax.driver.core.DataType.Name.BLOB;
import static com.datastax.driver.core.DataType.Name.BOOLEAN;
import static com.datastax.driver.core.DataType.Name.COUNTER;
import static com.datastax.driver.core.DataType.Name.CUSTOM;
import static com.datastax.driver.core.DataType.Name.DECIMAL;
import static com.datastax.driver.core.DataType.Name.DOUBLE;
import static com.datastax.driver.core.DataType.Name.FLOAT;
import static com.datastax.driver.core.DataType.Name.INET;
import static com.datastax.driver.core.DataType.Name.INT;
import static com.datastax.driver.core.DataType.Name.LIST;
import static com.datastax.driver.core.DataType.Name.MAP;
import static com.datastax.driver.core.DataType.Name.SET;
import static com.datastax.driver.core.DataType.Name.TEXT;
import static com.datastax.driver.core.DataType.Name.TIMESTAMP;
import static com.datastax.driver.core.DataType.Name.TIMEUUID;
import static com.datastax.driver.core.DataType.Name.UUID;
import static com.datastax.driver.core.DataType.Name.VARCHAR;
import static com.datastax.driver.core.DataType.Name.VARINT;
import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isSupportedNativeType;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.type.Counter;

public class TypeMapper {

    private static final Map<Class<?>, Name> java2CQL = new HashMap<Class<?>, Name>();
    private static final Map<Name, Class<?>> cql2Java = new HashMap<Name, Class<?>>();
    private static final Map<Class<?>, Method> rowPropertyInvoker = new HashMap<Class<?>, Method>();

    static {
        // Bytes
        java2CQL.put(byte.class, BLOB);
        java2CQL.put(Byte.class, BLOB);
        java2CQL.put(byte[].class, BLOB);
        java2CQL.put(ByteBuffer.class, BLOB);

        // Boolean
        java2CQL.put(Boolean.class, BOOLEAN);
        java2CQL.put(boolean.class, BOOLEAN);

        // Date
        java2CQL.put(Date.class, TIMESTAMP);
        java2CQL.put(InternalTimeUUID.class, TIMEUUID);

        // Double
        java2CQL.put(Double.class, DOUBLE);
        java2CQL.put(double.class, DOUBLE);

        // Float
        java2CQL.put(Float.class, FLOAT);
        java2CQL.put(float.class, FLOAT);
        java2CQL.put(BigDecimal.class, DECIMAL);

        // InetAddress
        java2CQL.put(InetAddress.class, INET);

        // Integer
        java2CQL.put(BigInteger.class, VARINT);
        java2CQL.put(Integer.class, INT);
        java2CQL.put(int.class, INT);

        // Long
        java2CQL.put(Long.class, BIGINT);
        java2CQL.put(long.class, BIGINT);

        // String
        java2CQL.put(String.class, TEXT);

        // UUID
        java2CQL.put(UUID.class, UUID);

        // Collections & Map
        java2CQL.put(List.class, LIST);
        java2CQL.put(Set.class, SET);
        java2CQL.put(Map.class, MAP);

        // Counter
        java2CQL.put(Counter.class, COUNTER);

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

        try {
            // Bytes
            rowPropertyInvoker.put(byte.class, Row.class.getDeclaredMethod("getBytes", String.class));
            rowPropertyInvoker.put(Byte.class, Row.class.getDeclaredMethod("getBytes", String.class));
            rowPropertyInvoker.put(byte[].class, Row.class.getDeclaredMethod("getBytes", String.class));
            rowPropertyInvoker.put(ByteBuffer.class, Row.class.getDeclaredMethod("getBytes", String.class));

            // Boolean
            rowPropertyInvoker.put(Boolean.class, Row.class.getDeclaredMethod("getBool", String.class));
            rowPropertyInvoker.put(boolean.class, Row.class.getDeclaredMethod("getBool", String.class));

            // Date
            rowPropertyInvoker.put(Date.class, Row.class.getDeclaredMethod("getDate", String.class));

            // Double
            rowPropertyInvoker.put(Double.class, Row.class.getDeclaredMethod("getDouble", String.class));
            rowPropertyInvoker.put(double.class, Row.class.getDeclaredMethod("getDouble", String.class));

            // Float
            rowPropertyInvoker.put(Float.class, Row.class.getDeclaredMethod("getFloat", String.class));
            rowPropertyInvoker.put(float.class, Row.class.getDeclaredMethod("getFloat", String.class));
            rowPropertyInvoker.put(BigDecimal.class, Row.class.getDeclaredMethod("getDecimal", String.class));

            // InetAddress
            rowPropertyInvoker.put(InetAddress.class, Row.class.getDeclaredMethod("getInet", String.class));

            // Integer
            rowPropertyInvoker.put(BigInteger.class, Row.class.getDeclaredMethod("getVarint", String.class));
            rowPropertyInvoker.put(Integer.class, Row.class.getDeclaredMethod("getInt", String.class));
            rowPropertyInvoker.put(int.class, Row.class.getDeclaredMethod("getInt", String.class));

            // Long
            rowPropertyInvoker.put(Long.class, Row.class.getDeclaredMethod("getLong", String.class));
            rowPropertyInvoker.put(long.class, Row.class.getDeclaredMethod("getLong", String.class));

            // String
            rowPropertyInvoker.put(String.class, Row.class.getDeclaredMethod("getString", String.class));

            // UUID
            rowPropertyInvoker.put(UUID.class, Row.class.getDeclaredMethod("getUUID", String.class));
            rowPropertyInvoker.put(InternalTimeUUID.class, Row.class.getDeclaredMethod("getUUID", String.class));

        } catch (Exception e) {
            throw new AchillesException("Cannot find getter in '" + Row.class.getCanonicalName() + "' ", e);
        }

    }

    public static DataType.Name toCQLType(Class<?> javaType) {
        Name name = java2CQL.get(javaType);

        // Custom object will be JSON serialized
        if (name == null) {
            name = TEXT;
        }

        return name;
    }

    public static Class<?> toJavaType(Name cqlType) {
        return cql2Java.get(cqlType);
    }

    public static Method getRowMethod(Class<?> javaType) {
        Method method = rowPropertyInvoker.get(javaType);

        // Custom object will be JSON serialized
        if (method == null) {
            method = rowPropertyInvoker.get(String.class);
        }
        return method;
    }

    public static Class<?> toCompatibleJavaType(Class<?> originalClass) {
        if (isSupportedNativeType(originalClass)) {
            return originalClass;
        } else {
            return String.class;
        }
    }
}
