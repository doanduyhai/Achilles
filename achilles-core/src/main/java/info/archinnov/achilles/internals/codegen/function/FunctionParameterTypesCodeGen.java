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
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.parser.context.FunctionsContext;
import info.archinnov.achilles.internals.utils.TypeNameHelper;

public abstract class FunctionParameterTypesCodeGen {

    public static String PARTITION_KEYS_TYPE = "PartitionKeys" + FUNCTION_TYPE_SUFFIX;

    public abstract List<TypeSpec> buildParameterTypesClasses(FunctionsContext functionContext);

    protected abstract void enhanceGeneratedType(TypeSpec.Builder builder, TypeName typeName);


    protected List<TypeSpec> buildParameterTypesClassesInternal(FunctionsContext functionContext) {

        final Set<TypeName> uniqueTypeNames = functionContext.allUsedTypes
                .stream()
                .map(TypeName::box)
                .collect(Collectors.toSet());

        List<TypeSpec> typeSpecs = uniqueTypeNames
                .stream()
                .map(typeName -> {
                    final TypeSpec.Builder builder = TypeSpec.classBuilder(TypeNameHelper.asString(typeName) + FUNCTION_TYPE_SUFFIX)
                            .superclass(genericType(ABSTRACT_CQL_COMPATIBLE_TYPE, typeName))
                            .addSuperinterface(FUNCTION_CALL)
                            .addModifiers(Modifier.PUBLIC)
                            .addMethod(buildConstructor(typeName))
                            .addMethod(buildIsFunctionCall());

                    if (typeName.equals(LIST) || typeName.equals(SET) || typeName.equals(MAP)) {
                        builder.addAnnotation(AnnotationSpec
                                .builder(SuppressWarnings.class)
                                .addMember("value", "$S", "rawtypes")
                                .build());
                    }

                    enhanceGeneratedType(builder, typeName);
                    return builder.build();
                })
                .collect(Collectors.toList());

        /**
         * Build here the special PartitionKey_Type
         * useful for SystemFunctions.token(xxx_AchillesMeta.COLUMNS.PARTITION_KEYS, "tokens")
         */
        TypeSpec partitionKeyType = TypeSpec.classBuilder(PARTITION_KEYS_TYPE)
                .superclass(genericType(ABSTRACT_CQL_COMPATIBLE_TYPE, STRING))
                .addSuperinterface(FUNCTION_CALL)
                .addModifiers(Modifier.PUBLIC)
                /**
                 * private final List<String> partitionKeys;
                 */
                .addField(FieldSpec
                        .builder(ParameterizedTypeName.get(LIST, STRING), "partitionKeys", Modifier.PRIVATE, Modifier.FINAL)
                        .build()
                )
                /**
                 * public PartitionKeys_Type(List<String> partitionKeys) {
                 *      this.partitionKeys = partitionKeys;
                 * }
                 */
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(ParameterizedTypeName.get(LIST, STRING), "partitionKeys", Modifier.FINAL)
                        .addStatement("this.partitionKeys = partitionKeys")
                        .build())
                .addMethod(MethodSpec.methodBuilder("isFunctionCall")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(BOOLEAN)
                        .addStatement("return true")
                        .build())
                /**
                 * @Override
                 *   public List<String> getValue() {
                 *     return this.partitionKeys;
                 *   }
                 */
                .addMethod(MethodSpec.methodBuilder("getValue")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .returns(ParameterizedTypeName.get(LIST, STRING))
                        .addStatement("return this.partitionKeys")
                        .build())
                .build();

        typeSpecs.add(partitionKeyType);

        return typeSpecs;
    }

    protected MethodSpec buildConstructor(TypeName typeName) {
        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PROTECTED)
                .addParameter(genericType(OPTIONAL, typeName), "value", Modifier.FINAL)
                .addStatement("this.value = value")
                .build();
    }

    protected MethodSpec buildIsFunctionCall() {
        return MethodSpec.methodBuilder("isFunctionCall")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(BOOLEAN)
                .addStatement("return false")
                .build();
    }

}
