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

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.metamodel.functions.UDFSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;

public class FunctionsRegistryCodeGen {

    public static TypeSpec generateFunctionsRegistryClass(String className, List<UDFSignature> udfSignatures) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        if (className.equals(SYSTEM_FUNCTIONS_CLASS)) {
            builder.addJavadoc("This class is the common registry for all system functions");
        } else {
            builder.addJavadoc("This class is the common registry for all registered user-defined functions");
        }

        udfSignatures.forEach(signature -> builder.addMethod(buildMethodForFunction(signature)));

        return builder.build();
    }

    private static MethodSpec buildMethodForFunction(UDFSignature signature) {
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
                    .addStatement("return $S", TypeUtils.getNativeDataTypeFor(signature.returnType))
                    .build());
        }

        final TypeSpec anonymousClass = anonymousClassBuilder.build();

        return builder.addStatement("return $L", anonymousClass)
                .build();
    }
}
