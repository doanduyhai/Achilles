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

package info.archinnov.achilles.internals.codegen.function;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.context.FunctionSignature.FunctionParamSignature;

public class InternalSystemFunctionRegistry {


    public static final List<FunctionSignature> SYSTEM_FUNCTIONS = new ArrayList<>();
    public static final List<String> SYSTEM_FUNCTIONS_NAME = new ArrayList<>();
    public static final String FQCN_PATTERN = ".+\\.([a-zA-Z0-9]+)".intern();

    public static final List<String> FORBIDDEN_KEYSPACES = Arrays.asList("system", "system_schema", "system_auth",
        "system_distributed", "system_traces", "dse_perf", "dse_system", "dse_security", "cfs", "cfs_archive",
        "solr_admin", "\"OpsCenter\"", "\"HiveMetaStore\"");

    public static final List<TypeName> NUMERIC_TYPES = Arrays.asList(OBJECT_BYTE, OBJECT_SHORT, BIG_INT, OBJECT_LONG,
            OBJECT_FLOAT, OBJECT_DOUBLE, BIG_DECIMAL, OBJECT_INT);

    static {
        NUMERIC_TYPES
                .stream()
                .forEach(targetType -> {
                    final String targetCQLDataType = DRIVER_TYPES_FUNCTION_PARAM_MAPPING.get(targetType);
                    final FunctionParamSignature returnTypeSignature = new FunctionParamSignature("returnType", targetType, targetType, targetCQLDataType);
                    final List<FunctionParamSignature> paramsSignature = asList(new FunctionParamSignature("input", targetType, targetType, targetCQLDataType));
                    SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "sum", "sum", returnTypeSignature, paramsSignature));
                    SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "avg", "avg", returnTypeSignature, paramsSignature));
                });


        final FunctionParamSignature timestampReturnSignature = new FunctionParamSignature("returnType", JAVA_UTIL_DATE, JAVA_UTIL_DATE, "timestamp");
        final FunctionParamSignature dateReturnSignature = new FunctionParamSignature("returnType", JAVA_DRIVER_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE, "date");
        final FunctionParamSignature timeuuidReturnSignature = new FunctionParamSignature("returnType", UUID, UUID, "timeuuid");
        final FunctionParamSignature uuidReturnSignature = new FunctionParamSignature("returnType", UUID, UUID, "uuid");
        final FunctionParamSignature byteBufferReturnSignature = new FunctionParamSignature("returnType", BYTE_BUFFER, BYTE_BUFFER, "blob");

        final FunctionParamSignature timestampInput = new FunctionParamSignature("input", JAVA_UTIL_DATE, JAVA_UTIL_DATE, "timestamp");
        final FunctionParamSignature uuidInput = new FunctionParamSignature("input", UUID, UUID, "uuid");
        final FunctionParamSignature timeUuidInput = new FunctionParamSignature("input", UUID, UUID, "timeuuid");
        final FunctionParamSignature dateInput = new FunctionParamSignature("input", JAVA_DRIVER_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE, "date");
        final FunctionParamSignature inetInput = new FunctionParamSignature("input", INET_ADDRESS, INET_ADDRESS, "inet");
        final FunctionParamSignature booleanInput = new FunctionParamSignature("input", OBJECT_BOOLEAN, OBJECT_BOOLEAN, "boolean");
        final FunctionParamSignature textInput = new FunctionParamSignature("input", STRING, STRING, "text");
        final FunctionParamSignature longInput = new FunctionParamSignature("input", OBJECT_LONG, OBJECT_LONG, "long");
        final FunctionParamSignature tinyintInput = new FunctionParamSignature("input", OBJECT_BYTE, OBJECT_BYTE, "tinyint");
        final FunctionParamSignature varintInput = new FunctionParamSignature("input", BIG_INT, BIG_INT, "varint");
        final FunctionParamSignature byteBufferInput = new FunctionParamSignature("input", BYTE_BUFFER, BYTE_BUFFER, "varint");

        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "minTimeuuid", timeuuidReturnSignature, asList(timestampInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "maxTimeuuid", timeuuidReturnSignature, asList(timestampInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toDate", dateReturnSignature, asList(timeUuidInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toDate", dateReturnSignature, asList(timestampInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toTimestamp", timestampReturnSignature, asList(timeUuidInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toTimestamp", timestampReturnSignature, asList(dateInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toUnixTimestamp", varintInput, asList(timeUuidInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toUnixTimestamp", varintInput, asList(timestampInput)));

        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "asciiAsBlob", byteBufferReturnSignature, asList(textInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "bigintAsBlob", byteBufferReturnSignature, asList(longInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "booleanAsBlob", byteBufferReturnSignature, asList(booleanInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "counterAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", OBJECT_LONG, OBJECT_LONG, "counter"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "dateAsBlob", byteBufferReturnSignature, asList(dateInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "decimalAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", BIG_DECIMAL, BIG_DECIMAL, "decimal"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "doubleAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", OBJECT_DOUBLE, OBJECT_DOUBLE, "double"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "floatAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", OBJECT_FLOAT, OBJECT_FLOAT, "float"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "inetAsBlob", byteBufferReturnSignature, asList(inetInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "intAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", OBJECT_INT, OBJECT_INT, "int"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "smallintAsBlob", byteBufferReturnSignature, asList(new FunctionParamSignature("input", OBJECT_SHORT, OBJECT_SHORT, "short"))));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "textAsBlob", byteBufferReturnSignature, asList(textInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timeAsBlob", byteBufferReturnSignature, asList(longInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timestampAsBlob", byteBufferReturnSignature, asList(timestampInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timeuuidAsBlob", byteBufferReturnSignature, asList(timeUuidInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "tinyintAsBlob", byteBufferReturnSignature, asList(tinyintInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "uuidAsBlob", byteBufferReturnSignature, asList(uuidInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "varcharAsBlob", byteBufferReturnSignature, asList(textInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "varintAsBlob", byteBufferReturnSignature, asList(varintInput)));

        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsAscii", new FunctionParamSignature("returnType", STRING, STRING, "ascii"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsBigint", new FunctionParamSignature("returnType", OBJECT_LONG, OBJECT_LONG, "bigint"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsBoolean", new FunctionParamSignature("returnType", OBJECT_BOOLEAN, OBJECT_BOOLEAN, "boolean"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsCounter", new FunctionParamSignature("returnType", OBJECT_LONG, OBJECT_LONG, "counter"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDate", new FunctionParamSignature("returnType", JAVA_DRIVER_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE, "date"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDecimal", new FunctionParamSignature("returnType", BIG_DECIMAL, BIG_DECIMAL, "decimal"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDouble", new FunctionParamSignature("returnType", OBJECT_DOUBLE, OBJECT_DOUBLE, "double"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsFloat", new FunctionParamSignature("returnType", OBJECT_FLOAT, OBJECT_FLOAT, "float"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsInet", new FunctionParamSignature("returnType", INET_ADDRESS, INET_ADDRESS, "inet"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsInt", new FunctionParamSignature("returnType", OBJECT_INT, OBJECT_INT, "int"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsSmallint", new FunctionParamSignature("returnType", OBJECT_SHORT, OBJECT_SHORT, "smallint"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsText", new FunctionParamSignature("returnType", STRING, STRING, "text"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTime", new FunctionParamSignature("returnType", OBJECT_LONG, OBJECT_LONG, "time"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTimestamp", new FunctionParamSignature("returnType", JAVA_UTIL_DATE, JAVA_UTIL_DATE, "timestamp"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTimeUUID", new FunctionParamSignature("returnType", UUID, UUID, "timeuuid"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTinyint", new FunctionParamSignature("returnType", OBJECT_BYTE, OBJECT_BYTE, "tinyint"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsUUID", new FunctionParamSignature("returnType", UUID, UUID, "uuid"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsVarchar", new FunctionParamSignature("returnType", STRING, STRING, "varchar"), asList(byteBufferInput)));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsVarint", new FunctionParamSignature("returnType", BIG_INT, BIG_INT, "varint"), asList(byteBufferInput)));

        TypeUtils.NATIVE_TYPES_2_1
                .stream()
                // Exclude collection types
                .filter(x -> (!x.equals(LIST) && !x.equals(SET) && !x.equals(MAP)))
                // Exclude UDT & Tuple types
                .filter(x -> (!x.equals(JAVA_DRIVER_UDT_VALUE_TYPE) && !x.equals(JAVA_DRIVER_TUPLE_VALUE_TYPE)))
                .forEach(nativeType -> {
                    final String targetCQLDataType = DRIVER_TYPES_FUNCTION_PARAM_MAPPING.get(nativeType);
                    final FunctionParamSignature returnType = new FunctionParamSignature("returnType", nativeType, nativeType, targetCQLDataType);
                    SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "min", returnType, asList(new FunctionParamSignature("input", nativeType, nativeType, targetCQLDataType))));
                    SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "max", returnType, asList(new FunctionParamSignature("input", nativeType, nativeType, targetCQLDataType))));
                });

        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "uuid", uuidReturnSignature, asList()));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "now", timeuuidReturnSignature, asList()));
        SYSTEM_FUNCTIONS.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "count", new FunctionParamSignature("returnType", OBJECT_LONG, OBJECT_LONG, "long"), asList()));

        SYSTEM_FUNCTIONS_NAME.addAll(SYSTEM_FUNCTIONS.stream().map(x -> x.name.toLowerCase()).collect(toList()));
        SYSTEM_FUNCTIONS_NAME.add("token");
        SYSTEM_FUNCTIONS_NAME.add("ttl");
        SYSTEM_FUNCTIONS_NAME.add("writetime");
    }
}
