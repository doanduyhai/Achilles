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

package info.archinnov.achilles.internals.codegen.function.cassandra3_2;

import static info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry.FQCN_PATTERN;
import static info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry.NUMERIC_TYPES;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;

import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.function.cassandra2_2.FunctionsRegistryCodeGen2_2;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;

public class FunctionsRegistryCodeGen3_2 extends FunctionsRegistryCodeGen2_2 {

    @Override
    public TypeSpec generateFunctionsRegistryClass(String className, List<FunctionSignature> functionSignatures) {

        final ArrayList<FunctionSignature> copyOfFunctionSignatures = new ArrayList<>(functionSignatures);
        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        if (className.equals(SYSTEM_FUNCTIONS_CLASS)) {
            addNewSystemFunctions(copyOfFunctionSignatures);
            builder.addJavadoc("This class is the common registry for all system functions");
            buildAcceptAllMethodsForSystemFunction().forEach(builder::addMethod);
        } else {
            builder.addJavadoc("This class is the common registry for all registered user-defined functions");
        }

        copyOfFunctionSignatures.forEach(signature -> builder.addMethod(super.buildMethodForFunction(signature)));

        return builder.build();
    }

    private void addNewSystemFunctions(List<FunctionSignature> functionSignatures) {
        NUMERIC_TYPES
                .stream()
                .forEach(targetType -> {
                    final String name = upperCaseFirst(targetType.toString().replaceAll(FQCN_PATTERN, "$1".intern()));
                    NUMERIC_TYPES.stream().filter(x -> !x.equals(targetType)).forEach(sourceType -> {
                        final String targetCQLDataType = DRIVER_TYPES_FUNCTION_PARAM_MAPPING.get(targetType);
                        final FunctionSignature.FunctionParamSignature returnTypeSignature = new FunctionSignature.FunctionParamSignature("returnType", targetType, targetType, targetCQLDataType);
                        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAs" + name, returnTypeSignature,
                                asList(new FunctionSignature.FunctionParamSignature("input", sourceType, sourceType, targetCQLDataType))));
                    });

                    final String targetCQLDataType = DRIVER_TYPES_FUNCTION_PARAM_MAPPING.get(targetType);
                    functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", new FunctionSignature.FunctionParamSignature("returnType", STRING, STRING, "text"),
                            asList(new FunctionSignature.FunctionParamSignature("input", targetType, targetType, targetCQLDataType))));
                });


        final FunctionSignature.FunctionParamSignature stringReturnSignature = new FunctionSignature.FunctionParamSignature("returnType", STRING, STRING, "text");
        final FunctionSignature.FunctionParamSignature timestampReturnSignature = new FunctionSignature.FunctionParamSignature("returnType", JAVA_UTIL_DATE, JAVA_UTIL_DATE, "timestamp");
        final FunctionSignature.FunctionParamSignature dateReturnSignature = new FunctionSignature.FunctionParamSignature("returnType", JAVA_DRIVER_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE, "date");

        final FunctionSignature.FunctionParamSignature timestampInput = new FunctionSignature.FunctionParamSignature("input", JAVA_UTIL_DATE, JAVA_UTIL_DATE, "timestamp");
        final FunctionSignature.FunctionParamSignature uuidInput = new FunctionSignature.FunctionParamSignature("input", UUID, UUID, "uuid");
        final FunctionSignature.FunctionParamSignature timeUuidInput = new FunctionSignature.FunctionParamSignature("input", UUID, UUID, "timeuuid");
        final FunctionSignature.FunctionParamSignature dateInput = new FunctionSignature.FunctionParamSignature("input", JAVA_DRIVER_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE, "date");
        final FunctionSignature.FunctionParamSignature inetInput = new FunctionSignature.FunctionParamSignature("input", INET_ADDRESS, INET_ADDRESS, "inet");
        final FunctionSignature.FunctionParamSignature booleanInput = new FunctionSignature.FunctionParamSignature("input", OBJECT_BOOLEAN, OBJECT_BOOLEAN, "boolean");

        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", stringReturnSignature, asList(booleanInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", stringReturnSignature, asList(inetInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", stringReturnSignature, asList(uuidInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", stringReturnSignature, asList(timestampInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsText", stringReturnSignature, asList(dateInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsTimestamp", timestampReturnSignature, asList(timestampInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsTimestamp", timestampReturnSignature, asList(timeUuidInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsDate", dateReturnSignature, asList(timestampInput)));
        functionSignatures.add(new FunctionSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "cast", "castAsDate", dateReturnSignature, asList(timeUuidInput)));
    }
}