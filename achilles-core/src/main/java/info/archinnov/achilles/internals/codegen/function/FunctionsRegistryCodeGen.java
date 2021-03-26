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

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.OBJECT;
import static info.archinnov.achilles.internals.codegen.function.FunctionParameterTypesCodeGen.PARTITION_KEYS_TYPE;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;

public abstract class FunctionsRegistryCodeGen {

    public TypeSpec generateFunctionsRegistryClass(String className, List<FunctionSignature> functionSignatures) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        if (className.equals(SYSTEM_FUNCTIONS_CLASS)) {
            builder.addJavadoc("This class is the common registry for all system functions");
            buildAcceptAllMethodsForSystemFunction().forEach(builder::addMethod);
        }

        return builder.build();
    }

    protected MethodSpec buildMethodForFunction(FunctionSignature signature) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(signature.methodName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", signature.getFunctionName())
                .returns(signature.returnTypeForFunctionParam())
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST);

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

        // CAST functions
        if (signature.getFunctionName().equals("cast")) {
            anonymousClassBuilder.addMethod(MethodSpec
                    .methodBuilder("targetCQLTypeName")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .returns(DATATYPE)
                    .addStatement("return $T.$L()", DATATYPE, TypeUtils.getNativeDataTypeFor(signature.returnTypeSignature.targetCQLTypeName))
                    .build());
        }

        final TypeSpec anonymousClass = anonymousClassBuilder.build();

        return builder.addStatement("return $L", anonymousClass)
                .build();
    }

    protected List<MethodSpec> buildAcceptAllMethodsForSystemFunction() {
        final List<MethodSpec> methods = new ArrayList<>();
        final TypeName LONG_TYPE = TypeUtils.determineTypeForFunctionParam(OBJECT_LONG);
        final TypeName INT_TYPE = TypeUtils.determineTypeForFunctionParam(OBJECT_INT);

        final TypeVariableName typeVariableName = TypeVariableName.get("T", ABSTRACT_CQL_COMPATIBLE_TYPE, FUNCTION_CALL);

        final AnnotationSpec unchecked = AnnotationSpec.builder(ClassName.get(SuppressWarnings.class))
                .addMember("value", "$S", "rawtypes")
                .build();

        //TODO To remove when upgrading to major version
        //Legacy Token function
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

        final MethodSpec.Builder tokenFunctionBuilder = MethodSpec.methodBuilder("token")
                .addTypeVariable(typeVariableName)
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "token")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'token' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'token' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input")
                .addStatement("return $L", tokenAnonClassBuilder.build());

        methods.add(tokenFunctionBuilder.build());


        //Type-safe token function
        final MethodSpec.Builder typeSafeTokenFunctionBuilder = MethodSpec.methodBuilder("token")
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "token")
                .returns(LONG_TYPE)
                .addParameter(ClassName.get(FUNCTION_PACKAGE, PARTITION_KEYS_TYPE), "partitionKeys", Modifier.FINAL)
                /**
                 *  final List<Object> params = new ArrayList<>();
                 *  for (String partitionKey : partitionKeys.getValue()) {
                 *       params.add(QueryBuilder.column(partitionKey));
                 *  }
                 */
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
                .beginControlFlow("for ($T partitionKey: partitionKeys.getValue())", STRING)
                    .addStatement("params.add($T.column(partitionKey))", QUERY_BUILDER)
                .endControlFlow()
                .addStatement("return $L", tokenAnonClassBuilder.build());

        methods.add(typeSafeTokenFunctionBuilder.build());


        //writetime function
        final MethodSpec.Builder writetimeFunctionBuilder = MethodSpec.methodBuilder("writetime")
                .addTypeVariable(typeVariableName)
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "writetime")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
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
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "countNotNull")
                .returns(LONG_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
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
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "countNotNull")
                .returns(INT_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
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
        return methods;
    }
}
