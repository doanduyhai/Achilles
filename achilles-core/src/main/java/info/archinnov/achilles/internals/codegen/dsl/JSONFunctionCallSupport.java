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

package info.archinnov.achilles.internals.codegen.dsl;

import static info.archinnov.achilles.internals.parser.TypeUtils.QUERY_BUILDER;
import static info.archinnov.achilles.internals.parser.TypeUtils.SCHEMA_NAME_PROVIDER;
import static info.archinnov.achilles.internals.parser.TypeUtils.STRING;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ClassSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.FieldSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType;
import info.archinnov.achilles.internals.parser.FieldParser;

public interface JSONFunctionCallSupport {

    default void buildSetFromJSONToRelationClass(TypeSpec.Builder relationClassBuilder,
                                                 FieldParser.FieldMetaSignature parsingResult,
                                                 TypeName newTypeName,
                                                 ReturnType returnType) {

        final String fieldName = parsingResult.context.fieldName;
        final String cqlColumn = parsingResult.context.quotedCqlColumn;

        final MethodSpec.Builder setFromJSONMethodBuilder = MethodSpec.methodBuilder("Set_FromJSON")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", cqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add($N)", fieldName)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            setFromJSONMethodBuilder.addStatement("return new $T(where)", newTypeName);
        } else {
            setFromJSONMethodBuilder.addStatement("return $T.this", newTypeName);
        }
        relationClassBuilder.addMethod(setFromJSONMethodBuilder.build());
    }

    default void buildEqFromJSONToRelationClass(TypeSpec.Builder relationClassBuilder,
                                                FieldSignatureInfo fieldInfo,
                                                ClassSignatureInfo nextSignature) {
        final String methodName = "Eq_FromJson";
        final MethodSpec fromJsonMethod = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L </strong>", fieldInfo.quotedCqlColumn, " = fromJson(?)")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.eq($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", fieldInfo.fieldName)
                .addStatement("encodedValues.add($N)", fieldInfo.fieldName)
                .returns(nextSignature.returnClassType)
                .addStatement("return new $T(where)", nextSignature.returnClassType)
                .build();

        relationClassBuilder.addMethod(fromJsonMethod);
    }

    default void buildIfEqFromJSONToConditionClass(TypeSpec.Builder conditionClassBuilder,
                                                   FieldSignatureInfo fieldSignatureInfo,
                                                   ClassSignatureInfo currentSignature) {
        String methodName = "Eq_FromJSON";
        final String fieldName = fieldSignatureInfo.fieldName;
        final String quotedCqlColumn = fieldSignatureInfo.quotedCqlColumn;
        MethodSpec fromJsonMethod = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an ... <strong>IF $L = fromJson(?)</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add($N)", fieldName)
                .addStatement("where.onlyIf($T.eq($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, quotedCqlColumn)
                .addStatement("return $T.this", currentSignature.returnClassType)
                .returns(currentSignature.returnClassType)
                .build();

        conditionClassBuilder.addMethod(fromJsonMethod);

    }

    default MethodSpec buildAllColumnsJSON(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumnsAsJSON_FromBaseTable")
                .addJavadoc("Generate ... * FROM ...")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final $T where = $L.json().all().from(meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName()), meta.getTableOrViewName()).where()",
                        whereTypeName, privateFieldName, "unknown_keyspace_for_")
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

    default MethodSpec buildAllColumnsJSONWithSchemaProvider(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumnsAsJSON_From")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate ... * FROM ... using the given SchemaNameProvider")
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $L.json().all().from(currentKeyspace, currentTable).where()", whereTypeName, privateFieldName)
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

}
