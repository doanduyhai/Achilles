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

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.OBJECT;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ClassSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.FieldSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.IndexFieldSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen.ParentSignature;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;

public interface JSONFunctionCallSupport {

    default TypeSpec.Builder buildSelectFromJSON(EntityMetaCodeGen.EntityMetaSignature signature,
                                         String className,
                                         TypeName selectWhereJSONTypeName,
                                         TypeName selectEndJSONTypeName) {

        return TypeSpec.classBuilder(className)
                .superclass(ABSTRACT_SELECT_FROM_JSON)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(SELECT_DOT_WHERE, "where")
                        .addParameter(OPTIONS, "cassandraOptions")
                        .addStatement("super(where, cassandraOptions)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("where")
                        .addJavadoc("Generate a SELECT ... FROM ... <strong>WHERE</strong> ...")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectWhereJSONTypeName)
                        .returns(selectWhereJSONTypeName)
                        .build())
                .addMethod(MethodSpec.methodBuilder("without_WHERE_Clause")
                        .addJavadoc("Generate a SELECT statement <strong>without</strong> the <strong>WHERE</strong> clause")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectEndJSONTypeName)
                        .returns(selectEndJSONTypeName)
                        .build());
    }

    default MethodSpec buildToJSONFunctionCall() {
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

        return toJSONFunctionBuilder.addStatement("return $L", toJSONAnonClassBuilder.build()).build();
    }

    default void buildSetFromJSONToRelationClass(ParentSignature parentSignature,
                                                 FieldMetaSignature fieldMeta,
                                                 TypeName newTypeName,
                                                 ReturnType returnType) {

        final String param = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMeta.context.fieldName + "_element")
                .orElse(fieldMeta.context.fieldName + "_element");

        final String cqlColumn = parentSignature.parentQuotedCQLColumn
                .map(x -> x + "." + fieldMeta.context.quotedCqlColumn)
                .orElse(fieldMeta.context.quotedCqlColumn);

        final MethodSpec.Builder setFromJSONMethodBuilder = MethodSpec.methodBuilder("Set_FromJSON")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", cqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param, Modifier.FINAL)
                .addStatement("where.with($T.of($S, $T.fromJson($T.bindMarker($S))))",
                        NON_ESCAPING_ASSIGNMENT, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add($N)", param)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            setFromJSONMethodBuilder.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            setFromJSONMethodBuilder.addStatement("return $T.this", newTypeName);
        }
        parentSignature.parentBuilder.addMethod(setFromJSONMethodBuilder.build());
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
                .addStatement("return new $T(where, cassandraOptions)", nextSignature.returnClassType)
                .build();

        relationClassBuilder.addMethod(fromJsonMethod);
    }

    default void buildJSONIndexRelationForMapEntry(TypeSpec.Builder relationClassBuilder,
                                               IndexFieldSignatureInfo indexFieldInfo,
                                               TypeName returnClassType,
                                               ReturnType returnType) {

        final String paramKey = indexFieldInfo.fieldName + "_JSONKey";
        final String paramValue = indexFieldInfo.fieldName + "_JSONValue";

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("ContainsEntry_FromJSON")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L[fromJson(?)] = fromJson(?)</strong>", indexFieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, paramKey)
                .addParameter(STRING, paramValue)
                .addStatement("where.and($T.of($S, $T.fromJson($T.bindMarker($S)), $T.fromJson($T.bindMarker($S))))",
                        MAP_ENTRY_CLAUSE, indexFieldInfo.quotedCqlColumn,
                        QUERY_BUILDER, QUERY_BUILDER, paramKey,
                        QUERY_BUILDER, QUERY_BUILDER, paramValue)
                .addStatement("boundValues.add($N)", paramKey)
                .addStatement("boundValues.add($N)", paramValue)
                .addStatement("encodedValues.add($N)", paramKey)
                .addStatement("encodedValues.add($N)", paramValue)
                .returns(returnClassType);

        if(returnType == ReturnType.THIS) {
            relationClassBuilder.addMethod(builder.addStatement("return $T.this", returnClassType).build());
        } else {
            relationClassBuilder.addMethod(builder.addStatement("return new $T(where, cassandraOptions)", returnClassType).build());
        }
    }

    default void buildJSONIndexRelationForMapKey(TypeSpec.Builder relationClassBuilder,
                                                     IndexFieldSignatureInfo indexFieldInfo,
                                                     TypeName returnClassType,
                                                     ReturnType returnType) {

        final String param = indexFieldInfo.fieldName + "_JSONKey";

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("ContainsKey_FromJSON")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L CONTAINS KEY fromJson(?)</strong>", indexFieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param)
                .addStatement("where.and($T.containsKey($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, indexFieldInfo.quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, indexFieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add($N)", param)
                .returns(returnClassType);

        if(returnType == ReturnType.THIS) {
            relationClassBuilder.addMethod(builder.addStatement("return $T.this", returnClassType).build());
        } else {
            relationClassBuilder.addMethod(builder.addStatement("return new $T(where, cassandraOptions)", returnClassType).build());
        }
    }

    default void buildJSONIndexRelationForMapValue(TypeSpec.Builder relationClassBuilder,
                                                       IndexFieldSignatureInfo indexFieldInfo,
                                                       TypeName returnClassType,
                                                       ReturnType returnType) {
        final String param = indexFieldInfo.fieldName + "_JSONValue";

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("ContainsValue_FromJSON")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L CONTAINS fromJson(?)</strong>", indexFieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param)
                .addStatement("where.and($T.contains($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, indexFieldInfo.quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, indexFieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add($N)", param)
                .returns(returnClassType);

        if(returnType == ReturnType.THIS) {
            relationClassBuilder.addMethod(builder.addStatement("return $T.this", returnClassType).build());
        } else {
            relationClassBuilder.addMethod(builder.addStatement("return new $T(where, cassandraOptions)", returnClassType).build());
        }
    }

    default void buildJSONIndexRelationForCollection(TypeSpec.Builder relationClassBuilder,
                                                         IndexFieldSignatureInfo indexFieldInfo,
                                                         TypeName returnClassType,
                                                         ReturnType returnType) {
        final String param = indexFieldInfo.fieldName + "_JSONElement";
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("Contains_FromJson")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L CONTAINS fromJson(?)</strong>", indexFieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param)
                .addStatement("where.and($T.contains($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, indexFieldInfo.quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, indexFieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add($N)", param)
                .returns(returnClassType);
        if(returnType == ReturnType.THIS) {
            relationClassBuilder.addMethod(builder.addStatement("return $T.this", returnClassType).build());
        } else {
            relationClassBuilder.addMethod(builder.addStatement("return new $T(where, cassandraOptions)", returnClassType).build());
        }
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
                .addStatement("return new $T(where, new $T())", newTypeName, OPTIONS)
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
                .addStatement("return new $T(where, $T.withSchemaNameProvider(schemaNameProvider))", newTypeName, OPTIONS)
                .returns(newTypeName)
                .build();
    }

}
