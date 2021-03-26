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
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import info.archinnov.achilles.internals.parser.TypeUtils;

public interface MultiColumnsSliceRestrictionCodeGen extends BaseSingleColumnRestriction {

    default void addSingleColumnSliceRestrictions(TypeSpec.Builder relationClassBuilder, FieldSignatureInfo fieldInfo,
                                                  ClassSignatureInfo nextSignature, ClassSignatureInfo lastSignature,
                                                  ReturnType returnType) {

        relationClassBuilder.addMethod(buildColumnInVarargs(nextSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildColumnRelation(GT, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildColumnRelation(GTE, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildColumnRelation(LT, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildColumnRelation(LTE, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildDoubleColumnRelation(GT, LT, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildDoubleColumnRelation(GT, LTE, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildDoubleColumnRelation(GTE, LT, lastSignature.returnClassType, fieldInfo, returnType))
                .addMethod(buildDoubleColumnRelation(GTE, LTE, lastSignature.returnClassType, fieldInfo, returnType));

    }

    default void addMultipleColumnsSliceRestrictions(TypeSpec.Builder parentClassBuilder,
                                                     String parentClassName,
                                                     List<FieldSignatureInfo> clusteringCols,
                                                     ClassSignatureInfo lastSignature,
                                                     ReturnType returnType) {
        // Tuple notation (col1, col2, ..., colN) < (:col1, :col2, ..., :colN)
        for (int i = 2; i <= clusteringCols.size(); i++) {
            final List<FieldSignatureInfo> fieldInfos = clusteringCols.stream().limit(i).collect(toList());
            final List<FieldSignatureInfo> fieldInfosMinusOne = clusteringCols.stream().limit(i - 1).collect(toList());

            String multiRelationName = fieldInfos
                    .stream().map(x -> x.fieldName).reduce((a, b) -> a + "_" + b)
                    .get();

            TypeName multiRelationClassTypeName = ClassName.get(DSL_PACKAGE, parentClassName
                    + "." + multiRelationName);

            TypeSpec multiRelationClass = TypeSpec.classBuilder(multiRelationName)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addMethod(buildTuplesColumnRelation(GT, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildTuplesColumnRelation(GTE, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildTuplesColumnRelation(LT, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildTuplesColumnRelation(LTE, lastSignature.returnClassType, fieldInfos, returnType))

                    .addMethod(buildSymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildSymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildSymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfos, returnType))
                    .addMethod(buildSymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfos, returnType))

                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne, returnType))

                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos, returnType))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos, returnType))
                    .build();

            parentClassBuilder.addType(multiRelationClass);
            parentClassBuilder.addMethod(buildRelationMethod(multiRelationName, multiRelationClassTypeName));
        }

    }

    default MethodSpec buildDoubleColumnRelation(String relation1, String relation2, TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final String methodName = upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);
        final String param1 = fieldInfo.fieldName + "_" + upperCaseFirst(relation1);
        final String param2 = fieldInfo.fieldName + "_" + upperCaseFirst(relation2);
        final String column1 = fieldInfo.cqlColumn + "_" + upperCaseFirst(relation2);
        final String column2 = fieldInfo.cqlColumn + "_" + upperCaseFirst(relation2);

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        fieldInfo.quotedCqlColumn, relationToSymbolForJavaDoc(relation1),
                        fieldInfo.quotedCqlColumn, relationToSymbolForJavaDoc(relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param1)
                .addParameter(fieldInfo.typeName, param2)
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation1, fieldInfo.quotedCqlColumn, QUERY_BUILDER, column1)
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation2, fieldInfo.quotedCqlColumn, QUERY_BUILDER, column2)
                .addStatement("boundValues.add($L)", param1)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldInfo.fieldName, param1, OPTIONAL)
                .addStatement("boundValues.add($L)", param2)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldInfo.fieldName, param2, OPTIONAL)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }

        return builder.build();
    }

    default MethodSpec buildDoubleTokenValueRelation(String relation1, String relation2, TypeName nextType, List<String> partitionKeyColumns, ReturnType returnType) {
        final String methodName = upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);
        final String fcall = partitionKeyColumns.stream().collect(Collectors.joining(",", "token(", ")"));

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        fcall, relationToSymbolForJavaDoc(relation1),
                        fcall, relationToSymbolForJavaDoc(relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeUtils.OBJECT_LONG, "tokenValue1")
                .addParameter(TypeUtils.OBJECT_LONG, "tokenValue2")
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation1, fcall, QUERY_BUILDER, "tokenValue1")
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation2, fcall, QUERY_BUILDER, "tokenValue2")
                .addStatement("boundValues.add($N)", "tokenValue1")
                .addStatement("encodedValues.add($N)", "tokenValue1")
                .addStatement("boundValues.add($N)", "tokenValue2")
                .addStatement("encodedValues.add($N)", "tokenValue2")
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }

        return builder.build();
    }

    default MethodSpec buildTuplesColumnRelation(String relation, TypeName nextType, List<FieldSignatureInfo> fieldInfos, ReturnType returnType) {
        final String methodName = upperCaseFirst(relation);

        StringJoiner paramsJoiner = new StringJoiner(",");
        StringJoiner dataTypeJoiner = new StringJoiner(",");
        fieldInfos
                .stream()
                .map(x -> x.quotedCqlColumn.replaceAll("\"", "\\\\\""))
                .forEach(x -> paramsJoiner.add("\"" + x + "\""));

        final String params = paramsJoiner.toString();
        final String dataTypes = dataTypeJoiner.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ?</strong>",
                        formatColumnTuplesForJavadoc(params), relationToSymbolForJavaDoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation, ARRAYS, params, ARRAYS, params, QUERY_BUILDER, COLLECTORS)
                .addStatement("final $T tupleType = rte.tupleTypeFactory.typeFor($L)", TUPLE_TYPE, dataTypes);

        for(FieldSignatureInfo x: fieldInfos) {
            builder.addParameter(x.typeName, x.fieldName, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", x.fieldName)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L, $T.of(cassandraOptions)))", x.fieldName, x.fieldName, OPTIONAL);
        }

        builder.returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildSymmetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos, ReturnType returnType) {
        final String methodName = upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);

        StringJoiner paramsJoinerRelation1AsString = new StringJoiner(",");
        StringJoiner paramsJoinerRelation2AsString = new StringJoiner(",");


        fieldInfos
                .stream()
                .map(x -> x.quotedCqlColumn.replaceAll("\"", "\\\\\""))
                .forEach(x -> {
                    paramsJoinerRelation1AsString.add("\"" + x + "\"");
                    paramsJoinerRelation2AsString.add("\"" + x + "\"");
                });

        final String paramsRelation1AsString = paramsJoinerRelation1AsString.toString();
        final String paramsRelation2AsString = paramsJoinerRelation2AsString.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        formatColumnTuplesForJavadoc(paramsRelation1AsString), relationToSymbolForJavaDoc(relation1),
                        formatColumnTuplesForJavadoc(paramsRelation2AsString), relationToSymbolForJavaDoc(relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation1, ARRAYS, paramsRelation1AsString, ARRAYS, paramsRelation1AsString, QUERY_BUILDER, COLLECTORS)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation2, ARRAYS, paramsRelation2AsString, ARRAYS, paramsRelation2AsString, QUERY_BUILDER, COLLECTORS);

        for(FieldSignatureInfo x: fieldInfos) {
            final String relation1Param = x.fieldName + "_" + upperCaseFirst(relation1);
            builder.addParameter(x.typeName, relation1Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation1Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L, $T.of(cassandraOptions)))", x.fieldName, relation1Param, OPTIONAL);
        }

        for(FieldSignatureInfo x: fieldInfos) {
            final String relation2Param = x.fieldName + "_" + upperCaseFirst(relation2);
            builder.addParameter(x.typeName, relation2Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation2Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L, $T.of(cassandraOptions)))", x.fieldName, relation2Param, OPTIONAL);
        }

        builder.returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }

        return builder.build();
    }

    default MethodSpec buildAsymmetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos1, List<FieldSignatureInfo> fieldInfos2, ReturnType returnType) {
        final String methodName =
                fieldInfos1
                        .stream()
                        .map(x -> x.fieldName)
                        .reduce((a, b) -> a + "_And_" + b)
                        .get() + "_" + upperCaseFirst(relation1) + "_And_" +
                        fieldInfos2
                                .stream()
                                .map(x -> x.fieldName)
                                .reduce((a, b) -> a + "_And_" + b)
                                .get() + "_" + upperCaseFirst(relation2);

        StringJoiner paramsJoinerRelation1AsString = new StringJoiner(",");
        StringJoiner paramsJoinerRelation2AsString = new StringJoiner(",");

        fieldInfos1
                .stream()
                .map(x -> x.quotedCqlColumn.replaceAll("\"", "\\\\\""))
                .forEach(x -> paramsJoinerRelation1AsString.add("\"" + x + "\""));

        fieldInfos2
                .stream()
                .map(x -> x.quotedCqlColumn.replaceAll("\"", "\\\\\""))
                .forEach(x -> paramsJoinerRelation2AsString.add("\"" + x + "\""));

        final String paramsRelation1AsString = paramsJoinerRelation1AsString.toString();
        final String paramsRelation2AsString = paramsJoinerRelation2AsString.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        formatColumnTuplesForJavadoc(paramsRelation1AsString), relationToSymbolForJavaDoc(relation1),
                        formatColumnTuplesForJavadoc(paramsRelation2AsString), relationToSymbolForJavaDoc(relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation1, ARRAYS, paramsRelation1AsString, ARRAYS, paramsRelation1AsString, QUERY_BUILDER, COLLECTORS)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation2, ARRAYS, paramsRelation2AsString, ARRAYS, paramsRelation2AsString, QUERY_BUILDER, COLLECTORS);

        for(FieldSignatureInfo x: fieldInfos1) {
            final String relation1Param = x.fieldName + "_" + upperCaseFirst(relation1);
            builder.addParameter(x.typeName, relation1Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation1Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L, $T.of(cassandraOptions)))", x.fieldName, relation1Param, OPTIONAL);
        }

        for(FieldSignatureInfo x: fieldInfos2) {
            final String relation2Param = x.fieldName + "_" + upperCaseFirst(relation2);
            builder.addParameter(x.typeName, relation2Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation2Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L, $T.of(cassandraOptions)))", x.fieldName, relation2Param, OPTIONAL);
        }


        builder.returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }

        return builder.build();
    }

}
