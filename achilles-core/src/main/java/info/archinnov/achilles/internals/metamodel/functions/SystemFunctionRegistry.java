/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

package info.archinnov.achilles.internals.metamodel.functions;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.metamodel.functions.UDFSignature.UDFParamSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;

public class SystemFunctionRegistry {


    public static final List<TypeName> EMPTY = new ArrayList<>();
    public static final List<UDFSignature> SYSTEM_FUNCTIONS = new ArrayList<>();
    public static final List<String> SYSTEM_FUNCTIONS_NAME = new ArrayList<>();
    public static final TypeName SYSTEM_FUNCTION_REGISTRY = ClassName.get(SystemFunctionRegistry.class);
    private static final String FQCN_PATTERN = ".+\\.([a-zA-Z0-9]+)".intern();

    public static final List<String> FORBIDDEN_KEYSPACES = Arrays.asList("system", "system_schema", "system_auth",
        "system_distributed", "system_traces", "dse_perf", "dse_system", "dse_security", "cfs", "cfs_archive",
        "solr_admin", "\"OpsCenter\"", "\"HiveMetaStore\"");

    private static final List<TypeName> NUMERIC_TYPES = Arrays.asList(OBJECT_BYTE, OBJECT_SHORT, BIG_INT, OBJECT_LONG,
            OBJECT_FLOAT, OBJECT_DOUBLE, BIG_DECIMAL, OBJECT_INT);

    static {

        NUMERIC_TYPES
                .stream()
                .forEach(targetType -> {
                    final String name = upperCaseFirst(targetType.toString().replaceAll(FQCN_PATTERN, "$1".intern()));
                    NUMERIC_TYPES.stream().filter(x -> !x.equals(targetType)).forEach(sourceType ->
                        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAs"+name, targetType, EMPTY, asList(new UDFParamSignature(sourceType, "input"))))
                    );

                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "sum", targetType, EMPTY, asList(new UDFParamSignature(targetType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "avg", targetType, EMPTY, asList(new UDFParamSignature(targetType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(targetType, "input"))));
                });

        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(INET_ADDRESS, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(OBJECT_BOOLEAN, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", STRING, EMPTY, asList(new UDFParamSignature(JAVA_DRIVER_LOCAL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsTimestamp", JAVA_UTIL_DATE, EMPTY, asList(new UDFParamSignature(JAVA_DRIVER_LOCAL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsTimestamp", JAVA_UTIL_DATE, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsDate", JAVA_DRIVER_LOCAL_DATE, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsDate", JAVA_DRIVER_LOCAL_DATE, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));


        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "minTimeuuid", UUID, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "maxTimeuuid", UUID, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toDate", JAVA_DRIVER_LOCAL_DATE, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toDate", JAVA_DRIVER_LOCAL_DATE, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toTimestamp", JAVA_UTIL_DATE, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toTimestamp", JAVA_UTIL_DATE, EMPTY, asList(new UDFParamSignature(JAVA_DRIVER_LOCAL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toUnixTimestamp", BIG_INT, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "toUnixTimestamp", BIG_INT, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));

        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "asciiAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(STRING, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "bigintAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_LONG, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "booleanAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_BOOLEAN, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "counterAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_LONG, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "dateAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(JAVA_DRIVER_LOCAL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "decimalAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(BIG_DECIMAL, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "doubleAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_DOUBLE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "floatAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_FLOAT, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "inetAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(INET_ADDRESS, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "intAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_INT, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "smallintAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_SHORT, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "textAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(STRING, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timeAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_LONG, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timestampAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(JAVA_UTIL_DATE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "timeuuidAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "tinyintAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(OBJECT_BYTE, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "uuidAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(UUID, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "varcharAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(STRING, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "varintAsBlob", BYTE_BUFFER, EMPTY, asList(new UDFParamSignature(BIG_INT, "input"))));

        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsAscii", STRING, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsBigint", OBJECT_LONG, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsBoolean", OBJECT_BOOLEAN, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsCounter", OBJECT_LONG, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDate", JAVA_DRIVER_LOCAL_DATE, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDecimal", BIG_DECIMAL, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsDouble", OBJECT_DOUBLE, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsFloat", OBJECT_FLOAT, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsInet", INET_ADDRESS, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsInt", OBJECT_INT, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsSmallint", OBJECT_SHORT, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsText", STRING, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTime", OBJECT_LONG, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTimestamp", JAVA_UTIL_DATE, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTimeUUID", UUID, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsTinyint", OBJECT_BYTE, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsUUID", UUID, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsVarchar", STRING, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "blobAsVarint", BIG_INT, EMPTY, asList(new UDFParamSignature(BYTE_BUFFER, "input"))));

        TypeUtils.NATIVE_TYPES
                .stream()
                // Exclude collection types
                .filter(x -> (!x.equals(LIST) && !x.equals(SET) && !x.equals(MAP)))
                .forEach(nativeType -> {

                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "token", OBJECT_LONG, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "ttl", OBJECT_INT, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "writetime", OBJECT_LONG, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "countNotNull", BIG_INT, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "min", nativeType, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                    SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "max", nativeType, EMPTY, asList(new UDFParamSignature(nativeType, "input"))));
                });

        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "uuid", UUID, EMPTY, asList()));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "now", UUID, EMPTY, asList()));
        SYSTEM_FUNCTIONS.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "count", BIG_INT, EMPTY, asList()));

        SYSTEM_FUNCTIONS_NAME.addAll(SYSTEM_FUNCTIONS.stream().map(x -> x.name.toLowerCase()).collect(toList()));

    }
}
