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

package info.archinnov.achilles.internals.codegen.function;

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.OBJECT;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.lang.model.element.Modifier;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.utils.TypeNameHelper;

public class FunctionsRegistryCodeGen {

    public static TypeSpec generateSystemFunctionsRegistryClass(String className, List<FunctionSignature> udfSignatures, Set<TypeName> allUsedTypes) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        builder.addJavadoc("This class is the common registry for all system functions");
        buildAcceptAllMethodsForSystemFunction().forEach(builder::addMethod);

        udfSignatures.forEach(signature -> builder.addMethod(buildMethodForFunction(signature)));

        return builder.build();
    }

    public static TypeSpec generateUserFunctionsRegistryClass(String className, List<FunctionSignature> udfSignatures) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        builder.addJavadoc("This class is the common registry for all registered user-defined functions");
        udfSignatures.forEach(signature -> builder.addMethod(buildMethodForFunction(signature)));

        return builder.build();
    }

    private static MethodSpec buildMethodForFunction(FunctionSignature signature) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(signature.methodName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", signature.getFunctionName())
                .returns(signature.returnTypeForFunctionParam())
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST);

        signature.parameterSignatures.forEach(param -> {
            final String paramName = param.name;
            builder.addParameter(param.typeForFunctionParam(), paramName, Modifier.FINAL)
                    .beginControlFlow("if ($L.isFunctionCall())", paramName)
                    .addStatement("params.add($L.buildRecursive())", paramName)
                    .nextControlFlow("else")
                    .addStatement("params.add($L.hasLiteralValue() ? $L.getValue() : $T.column((String)$L.getValue()))",
                            paramName, paramName, QUERY_BUILDER, paramName)
                    .endControlFlow();

        });


        final TypeSpec.Builder anonymousClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(signature.returnTypeForFunctionParam())
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", signature.getFunctionName())
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        if (signature.getFunctionName().equals("cast")) {
            anonymousClassBuilder.addMethod(MethodSpec
                    .methodBuilder("targetCQLTypeName")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .returns(STRING)
                    .addStatement("return $S", TypeUtils.getNativeDataTypeFor(signature.returnTypeSignature.targetCQLTypeName))
                    .build());
        }

        final TypeSpec anonymousClass = anonymousClassBuilder.build();

        return builder.addStatement("return $L", anonymousClass)
                .build();
    }

    private static List<MethodSpec> buildAcceptAllMethodsForSystemFunction() {
        final List<MethodSpec> methods = new ArrayList<>();
        final TypeName LONG_TYPE = TypeUtils.determineTypeForFunctionParam(OBJECT_LONG);
        final TypeName INT_TYPE = TypeUtils.determineTypeForFunctionParam(OBJECT_INT);

        final TypeVariableName typeVariableName = TypeVariableName.get("T", ABSTRACT_CQL_COMPATIBLE_TYPE, FUNCTION_CALL);

        //Token function
        final MethodSpec.Builder tokenFunctionBuilder = MethodSpec.methodBuilder("token")
                .addTypeVariable(typeVariableName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "token")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'token' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'token' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder tokenAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(LONG_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "token")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        methods.add(tokenFunctionBuilder.addStatement("return $L", tokenAnonClassBuilder.build()).build());


        //writetime function
        final MethodSpec.Builder writetimeFunctionBuilder = MethodSpec.methodBuilder("writetime")
                .addTypeVariable(typeVariableName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "writetime")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'writetime' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'writetime' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder writetimeAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(LONG_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "writetime")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        methods.add(writetimeFunctionBuilder.addStatement("return $L", writetimeAnonClassBuilder.build()).build());

        //count function
        final MethodSpec.Builder countNotNullFunctionBuilder = MethodSpec.methodBuilder("countNotNull")
                .addTypeVariable(typeVariableName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "countNotNull")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'countNotNull' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'countNotNull' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder countNotNullAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(LONG_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "count")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        methods.add(countNotNullFunctionBuilder.addStatement("return $L", countNotNullAnonClassBuilder.build()).build());

        //ttl function
        final MethodSpec.Builder ttllFunctionBuilder = MethodSpec.methodBuilder("ttl")
                .addTypeVariable(typeVariableName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "countNotNull")
                .returns(INT_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'ttl' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'ttl' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder ttlAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(INT_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "ttl")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        methods.add(ttllFunctionBuilder.addStatement("return $L", ttlAnonClassBuilder.build()).build());

        //toJson function
        final MethodSpec.Builder toJsonlFunctionBuilder = MethodSpec.methodBuilder("toJson")
                .addTypeVariable(typeVariableName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "toJson")
                .returns(INT_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'toJson' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'toJson' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder toJsonAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(INT_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "toJson")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());

        methods.add(toJsonlFunctionBuilder.addStatement("return $L", toJsonAnonClassBuilder.build()).build());

        return methods;
    }

    private static MethodSpec buildFromJsonMethodsForSystemFunction(TypeName targetTypeName) {

        QueryBuilder.insertInto("d").

        final String targetTypeAsString = TypeNameHelper.asString(targetTypeName);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("fromJson_To_" + targetTypeAsString)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function and return $S", "fromJson", targetTypeAsString)
                .returns(targetTypeName)
                // TODO make the input parameter not hard-coded as String but as an instance of String_Type
                // TODO for function composition when https://issues.apache.org/jira/browse/CASSANDRA-10783 is done
                .addParameter(STRING, "input", Modifier.FINAL)
                .addStatement("final $T params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("params.add($T.column($L))", QUERY_BUILDER, "input");

        final TypeSpec.Builder tokenAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(LONG_TYPE)
                .addMethod(MethodSpec
                        .methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("functionName")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(STRING)
                        .addStatement("return $S", "token")
                        .build())
                .addMethod(MethodSpec
                        .methodBuilder("parameters")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(genericType(LIST, OBJECT))
                        .addStatement("return params")
                        .build());
    }
}
