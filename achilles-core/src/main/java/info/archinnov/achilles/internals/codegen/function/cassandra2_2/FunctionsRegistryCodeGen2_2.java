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

package info.archinnov.achilles.internals.codegen.function.cassandra2_2;

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.OBJECT;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;

public class FunctionsRegistryCodeGen2_2 extends FunctionsRegistryCodeGen {

    @Override
    public TypeSpec generateFunctionsRegistryClass(String className, List<FunctionSignature> udfSignatures) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        if (className.equals(SYSTEM_FUNCTIONS_CLASS)) {
            builder.addJavadoc("This class is the common registry for all system functions");
            buildAcceptAllMethodsForSystemFunction().forEach(builder::addMethod);
        } else {
            builder.addJavadoc("This class is the common registry for all registered user-defined functions");
        }

        udfSignatures.forEach(signature -> builder.addMethod(super.buildMethodForFunction(signature)));

        return builder.build();
    }

    @Override
    protected List<MethodSpec> buildAcceptAllMethodsForSystemFunction() {
        final List<MethodSpec> methods = super.buildAcceptAllMethodsForSystemFunction();
        final TypeName STRING_TYPE = TypeUtils.determineTypeForFunctionParam(STRING);

        final TypeVariableName typeVariableName = TypeVariableName.get("T", ABSTRACT_CQL_COMPATIBLE_TYPE, FUNCTION_CALL);

        final AnnotationSpec unchecked = AnnotationSpec.builder(ClassName.get(SuppressWarnings.class))
                .addMember("value", "$S", "rawtypes")
                .build();

        //toJson function
        final MethodSpec.Builder toJSONFunctionBuilder = MethodSpec.methodBuilder("toJson")
                .addTypeVariable(typeVariableName)
                .addAnnotation(unchecked)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addJavadoc("Call $S function with given parameters", "toJson")
                .returns(STRING_TYPE)
                .addParameter(typeVariableName, "input", Modifier.FINAL)
                .addStatement("final $T<Object> params = new $T<>()", LIST, ARRAY_LIST)
                .addStatement("$T.validateFalse(input.isFunctionCall(), $S)", VALIDATOR, "Invalid argument for 'toJson' function, it does not accept function call as argument, only simple column")
                .addStatement("$T.validateFalse(input.hasLiteralValue(), $S)", VALIDATOR, "Invalid argument for 'toJson' function, it does not accept literal value as argument, only simple column")
                .addStatement("params.add($T.column((String)$L.getValue()))", QUERY_BUILDER, "input");

        final TypeSpec.Builder toJSONAnonClassBuilder = TypeSpec.anonymousClassBuilder("$T.empty()", OPTIONAL)
                .superclass(STRING_TYPE)
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

        methods.add(toJSONFunctionBuilder.addStatement("return $L", toJSONAnonClassBuilder.build()).build());

        return methods;
    }
}