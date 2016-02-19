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
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.parser.context.UDFContext;
import info.archinnov.achilles.internals.utils.TypeNameHelper;

public class FunctionParameterTypesCodeGen {

    public static List<TypeSpec> buildParameterTypesClasses(UDFContext UDFContext) {

        return UDFContext.allUsedTypes
            .stream()
            .map(typeName -> {
                TypeName boxed = typeName.box();
                return TypeSpec.classBuilder(TypeNameHelper.asString(boxed)+ FUNCTION_TYPE_SUFFIX)
                        .superclass(genericType(ABSTRACT_CQL_COMPATIBLE_TYPE, boxed))
                        .addSuperinterface(FUNCTION_CALL)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addMethod(buildConstructor(boxed))
                        .addMethod(buildIsFunctionCall())
                        //TODO Enable it when https://issues.apache.org/jira/browse/CASSANDRA-10783 is done
                        //.addMethod(buildStaticWrapperMethod(boxed))
                        .build();
            })
            .collect(Collectors.toList());

    }

    private static MethodSpec buildConstructor(TypeName typeName) {
        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PROTECTED)
                .addParameter(genericType(OPTIONAL, typeName), "value", Modifier.FINAL)
                .addStatement("this.value = value")
                .build();
    }

    private static MethodSpec buildIsFunctionCall() {
        return MethodSpec.methodBuilder("isFunctionCall")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(BOOLEAN)
                .addStatement("return false")
                .build();
    }

    private static MethodSpec buildStaticWrapperMethod(TypeName typeName) {
        TypeName returnType = ClassName.get(FUNCTION_PACKAGE, TypeNameHelper.asString(typeName)+ FUNCTION_TYPE_SUFFIX);
        return MethodSpec.methodBuilder("wrap")
                .addJavadoc("Wrap value $T", typeName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addParameter(typeName, "wrappedValue", Modifier.FINAL)
                .returns(returnType)
                .addStatement("$T.validateNotNull(wrappedValue, $S)", VALIDATOR,
                        format("The provided value for wrapper class %s should not be null", returnType))
                .addStatement("return new $T($T.of(wrappedValue))", returnType, OPTIONAL)
                .build();
    }
}
