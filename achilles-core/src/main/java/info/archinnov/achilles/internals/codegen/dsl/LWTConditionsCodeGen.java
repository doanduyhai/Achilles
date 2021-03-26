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

package info.archinnov.achilles.internals.codegen.dsl;

import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;

public interface LWTConditionsCodeGen {

    void augmentLWTConditionClass(TypeSpec.Builder conditionClassBuilder, FieldSignatureInfo fieldSignatureInfo, ClassSignatureInfo currentSignature);

    default void buildLWtConditionMethods(EntityMetaSignature signature, String parentFQCN, ClassSignatureInfo currentSignature, boolean hasCounter, TypeSpec.Builder parentBuilder) {
        if (!hasCounter) {
            signature.fieldMetaSignatures.stream()
                    .filter(x -> x.context.columnType == ColumnType.NORMAL || x.context.columnType == ColumnType.STATIC)
                    .forEach(x -> {

                        final FieldSignatureInfo fieldSignatureInfo = FieldSignatureInfo.of(x.context.fieldName, x.context.cqlColumn, x.sourceType);
                        final String conditionClassName = "If_" + upperCaseFirst(x.context.fieldName);
                        TypeName conditionClassTypeName = ClassName.get(DSL_PACKAGE, parentFQCN + "." + conditionClassName);

                        TypeSpec.Builder conditionClassBuilder = TypeSpec.classBuilder(conditionClassName)
                                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                                .addMethod(buildLWTConditionOnColumn(EQ, fieldSignatureInfo, currentSignature.returnClassType))
                                .addMethod(buildLWTConditionOnColumn(GT, fieldSignatureInfo, currentSignature.returnClassType))
                                .addMethod(buildLWTConditionOnColumn(GTE, fieldSignatureInfo, currentSignature.returnClassType))
                                .addMethod(buildLWTConditionOnColumn(LT, fieldSignatureInfo, currentSignature.returnClassType))
                                .addMethod(buildLWTConditionOnColumn(LTE, fieldSignatureInfo, currentSignature.returnClassType))
                                .addMethod(buildLWTNotEqual(fieldSignatureInfo, currentSignature.returnClassType));

                        augmentLWTConditionClass(conditionClassBuilder, fieldSignatureInfo, currentSignature);

                        parentBuilder.addType(conditionClassBuilder.build());
                        parentBuilder.addMethod(MethodSpec.methodBuilder("if_" + upperCaseFirst(x.context.fieldName))
                                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                                .addStatement("return new $T()", conditionClassTypeName)
                                .returns(conditionClassTypeName)
                                .build());
                    });

        }
    }

    default MethodSpec buildLWTConditionOnColumn(String relation, FieldSignatureInfo fieldSignatureInfo, TypeName currentType) {
        String methodName = upperCaseFirst(relation);
        final String fieldName = fieldSignatureInfo.fieldName;
        final String quotedCqlColumn = fieldSignatureInfo.quotedCqlColumn;
        return MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an ... <strong>IF $L $L ?</strong>", fieldName, relationToSymbolForJavaDoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldSignatureInfo.typeName, fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, fieldName, OPTIONAL)
                .addStatement("where.onlyIf($T.$L($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, relation, quotedCqlColumn, QUERY_BUILDER, quotedCqlColumn)
                .addStatement("return $T.this", currentType)
                .returns(currentType)
                .build();

    }

    default MethodSpec buildLWTNotEqual(FieldSignatureInfo fieldSignatureInfo, TypeName currentType) {
        String methodName =  "NotEq";
        final String fieldName = fieldSignatureInfo.fieldName;
        final String quotedCqlColumn = fieldSignatureInfo.quotedCqlColumn;

        return MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an  ... <strong>IF $L != ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldSignatureInfo.typeName, fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, fieldName, OPTIONAL)
                .addStatement("where.onlyIf($T.of($S, $T.bindMarker($S)))",
                        NOT_EQ, quotedCqlColumn, QUERY_BUILDER, quotedCqlColumn)
                .addStatement("return $T.this", currentType)
                .returns(currentType)
                .build();

    }
}
