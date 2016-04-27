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
import java.util.Set;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.FunctionsContext;
import info.archinnov.achilles.internals.utils.TypeNameHelper;

public class FunctionParameterTypesCodeGen {

    public static List<TypeSpec> buildParameterTypesClasses(FunctionsContext functionContext) {

        final Set<TypeName> uniqueTypeNames = functionContext.allUsedTypes
                .stream()
                .map(TypeName::box)
                .collect(Collectors.toSet());

        return uniqueTypeNames
            .stream()
            .map(typeName -> {

                //TODO Enable it when https://issues.apache.org/jira/browse/CASSANDRA-10783 is done
                //.addMethod(buildStaticWrapperMethod(boxed))

                final TypeSpec.Builder builder = TypeSpec.classBuilder(TypeNameHelper.asString(typeName) + FUNCTION_TYPE_SUFFIX)
                        .superclass(genericType(ABSTRACT_CQL_COMPATIBLE_TYPE, typeName))
                        .addSuperinterface(FUNCTION_CALL)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addMethod(buildConstructor(typeName))
                        .addMethod(buildIsFunctionCall());

                if (typeName.equals(LIST) || typeName.equals(SET) || typeName.equals(MAP)) {
                    builder.addAnnotation(AnnotationSpec
                            .builder(SuppressWarnings.class)
                            .addMember("value", "$S", "rawtypes")
                            .build());
                }

                return builder.build();
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
