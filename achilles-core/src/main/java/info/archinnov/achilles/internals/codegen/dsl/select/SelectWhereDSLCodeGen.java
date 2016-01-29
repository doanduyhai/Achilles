/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.codegen.dsl.select;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.type.tuples.Tuple;

public class SelectWhereDSLCodeGen extends AbstractDSLCodeGen {
    public static List<TypeSpec> buildWhereClasses(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.parsingResults);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.parsingResults);


        final Optional<FieldSignatureInfo> firstClustering = clusteringCols.stream().limit(1).findFirst();

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(SELECT_WHERE_DSL_SUFFIX,
                SELECT_END_DSL_SUFFIX, ABSTRACT_SELECT_WHERE_PARTITION, ABSTRACT_SELECT_WHERE);

        final List<ClassSignatureInfo> classesSignature =
                buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, clusteringCols,
                        WhereClauseFor.NORMAL);

        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(partitionKeys, classesSignature);
        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(signature, firstClustering, clusteringCols,
                classesSignature, lastSignature);

        final TypeSpec selectEndClass = buildSelectEndClass(signature, lastSignature, firstClustering);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(selectEndClass);
        return partitionKeysWhereClasses;
    }

    private static TypeSpec buildSelectEndClass(EntityMetaSignature signature, ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> firstClustering) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(lastSignature.className)
                .superclass(lastSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_WHERE))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(lastSignature))
                .addMethod(buildGetThis(lastSignature.classType));

        maybeBuildOrderingBy(lastSignature, firstClustering, builder);

        return builder.build();
    }

    private static void maybeBuildOrderingBy(ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> firstClustering, TypeSpec.Builder builder) {
        if (firstClustering.isPresent()) {

            final FieldSignatureInfo fieldSignatureInfo = firstClustering.get();
            final MethodSpec orderByAsc = MethodSpec
                    .methodBuilder("orderBy" + upperCaseFirst(fieldSignatureInfo.fieldName) + "Ascending")
                    .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>ORDER BY $L ASC</strong>", fieldSignatureInfo.cqlColum)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .returns(lastSignature.classType)
                    .addStatement("where.orderBy($T.asc($S))", QUERY_BUILDER, fieldSignatureInfo.cqlColum)
                    .addStatement("return this")
                    .build();

            final MethodSpec orderByDesc = MethodSpec
                    .methodBuilder("orderBy" + upperCaseFirst(fieldSignatureInfo.fieldName) + "Descending")
                    .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>ORDER BY $L DESC</strong>", fieldSignatureInfo.cqlColum)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .returns(lastSignature.classType)
                    .addStatement("where.orderBy($T.desc($S))", QUERY_BUILDER, fieldSignatureInfo.cqlColum)
                    .addStatement("return this")
                    .build();

            builder.addMethod(orderByAsc).addMethod(orderByDesc);
        }
    }

    private static MethodSpec buildLimit(ClassSignatureInfo lastSignature) {
        return MethodSpec.methodBuilder("limit")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>LIMIT :limit</strong>")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT.box(), "limit", Modifier.FINAL)
                .returns(lastSignature.classType)
                .addStatement("where.limit($T.bindMarker($S))", QUERY_BUILDER, "lim")
                .addStatement("boundValues.add($N)", "limit")
                .addStatement("encodedValues.add($N)", "limit")
                .addStatement("return this")
                .build();
    }


    private static List<TypeSpec> buildWhereClassesForPartitionKeys(List<FieldSignatureInfo> partitionKeys,
                                                                    List<ClassSignatureInfo> classesSignature) {
        if (partitionKeys.isEmpty()) {
            return new ArrayList<>();
        } else {
            final FieldSignatureInfo partitionKeyInfo = partitionKeys.remove(0);
            final ClassSignatureInfo classSignature = classesSignature.remove(0);
            final TypeSpec typeSpec = buildSelectWhereForPartitionKey(partitionKeyInfo, classSignature, classesSignature.get(0));
            final List<TypeSpec> typeSpecs = buildWhereClassesForPartitionKeys(partitionKeys, classesSignature);
            typeSpecs.add(0, typeSpec);
            return typeSpecs;
        }
    }

    private static TypeSpec buildSelectWhereForPartitionKey(FieldSignatureInfo partitionInfo,
                                                            ClassSignatureInfo classSignature,
                                                            ClassSignatureInfo nextSignature) {
        return TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_WHERE))
                .addMethod(buildColumnRelation(EQ, nextSignature.classType, partitionInfo))
                .addMethod(buildColumnInVarargs(nextSignature.classType, partitionInfo))
                .build();
    }


    private static List<TypeSpec> buildWhereClassesForClusteringColumns(EntityMetaSignature signature,
                                                                        Optional<FieldSignatureInfo> firstClusteringCol,
                                                                        List<FieldSignatureInfo> clusteringCols,
                                                                        List<ClassSignatureInfo> classesSignature,
                                                                        ClassSignatureInfo lastSignature) {
        if (clusteringCols.isEmpty()) {
            return new ArrayList<>();
        } else {
            final List<FieldSignatureInfo> copyClusteringCols = new ArrayList<>(clusteringCols);
            final List<ClassSignatureInfo> copyClassesSignature = new ArrayList<>(classesSignature);
            clusteringCols.remove(0);
            classesSignature.remove(0);
            final TypeSpec currentType = buildSelectWhereForClusteringColumn(signature, firstClusteringCol, copyClusteringCols, copyClassesSignature, lastSignature);
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(signature, firstClusteringCol, clusteringCols, classesSignature, lastSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    private static TypeSpec buildSelectWhereForClusteringColumn(EntityMetaSignature signature,
                                                                Optional<FieldSignatureInfo> firstClusteringCol,
                                                                List<FieldSignatureInfo> clusteringCols,
                                                                List<ClassSignatureInfo> classesSignature,
                                                                ClassSignatureInfo lastSignature) {

        final ClassSignatureInfo classSignature = classesSignature.get(0);
        final ClassSignatureInfo nextSignature = classesSignature.get(1);
        final FieldSignatureInfo clusteringColumnInfo = clusteringCols.get(0);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_WHERE))
                .addMethod(buildGetThis(classSignature.classType))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(classSignature))
                .addMethod(buildColumnRelation(EQ, nextSignature.classType, clusteringColumnInfo))
                .addMethod(buildColumnInVarargs(nextSignature.classType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(GT, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(GTE, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(LT, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(LTE, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GT, LT, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GT, LTE, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GTE, LT, lastSignature.classType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GTE, LTE, lastSignature.classType, clusteringColumnInfo));


        // Tuple notation (col1, col2, ..., colN) < (:col1, :col2, ..., :colN)
        for (int i = 2; i <= clusteringCols.size(); i++) {
            final List<FieldSignatureInfo> fieldInfos = clusteringCols.stream().limit(i).collect(toList());
            final List<FieldSignatureInfo> fieldInfosMinusOne = clusteringCols.stream().limit(i - 1).collect(toList());
            builder.addMethod(buildTuplesColumnRelation(GT, lastSignature.classType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(GTE, lastSignature.classType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(LT, lastSignature.classType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(LTE, lastSignature.classType, fieldInfos))

                    .addMethod(buildSymetricColumnDoubleRelation(GT, LT, lastSignature.classType, fieldInfos))
                    .addMethod(buildSymetricColumnDoubleRelation(GT, LTE, lastSignature.classType, fieldInfos))
                    .addMethod(buildSymetricColumnDoubleRelation(GTE, LT, lastSignature.classType, fieldInfos))
                    .addMethod(buildSymetricColumnDoubleRelation(GTE, LTE, lastSignature.classType, fieldInfos))

                    .addMethod(buildAsymetricColumnDoubleRelation(GT, LT, lastSignature.classType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymetricColumnDoubleRelation(GT, LTE, lastSignature.classType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymetricColumnDoubleRelation(GTE, LT, lastSignature.classType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymetricColumnDoubleRelation(GTE, LTE, lastSignature.classType, fieldInfos, fieldInfosMinusOne))

                    .addMethod(buildAsymetricColumnDoubleRelation(GT, LT, lastSignature.classType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymetricColumnDoubleRelation(GT, LTE, lastSignature.classType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymetricColumnDoubleRelation(GTE, LT, lastSignature.classType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymetricColumnDoubleRelation(GTE, LTE, lastSignature.classType, fieldInfosMinusOne, fieldInfos));
        }

        maybeBuildOrderingBy(classSignature, firstClusteringCol, builder);

        return builder.build();
    }

    private static MethodSpec buildTuplesColumnRelation(String relation, TypeName nextType, List<FieldSignatureInfo> fieldInfos) {
        final String methodName = fieldInfos
                .stream().map(x -> x.fieldName).reduce((a, b) -> a + "_And_" + b)
                .get() + "_" + upperCaseFirst(relation);

        StringJoiner paramsJoiner = new StringJoiner(",");
        StringJoiner dataTypeJoiner = new StringJoiner(",");
        StringJoiner encodedValuesJoiner = new StringJoiner(",");

        fieldInfos
                .stream()
                .map(x -> x.fieldName)
                .forEach(x -> {
                    dataTypeJoiner.add("meta." + x + ".getDataType()");
                    encodedValuesJoiner.add(x + "_encoded");
                });

        fieldInfos
                .stream()
                .map(x -> x.cqlColum)
                .forEach(x -> paramsJoiner.add("\"" + x + "\""));

        final String params = paramsJoiner.toString();
        final String dataTypes = dataTypeJoiner.toString();
        final String encodedValues = encodedValuesJoiner.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ?</strong>", params, relation)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.and($T.$L($T.asList($L), $T.asList($L).stream().map($T::bindMarker).collect($T.toList())))",
                        QUERY_BUILDER, relation, ARRAYS, params, ARRAYS, params, QUERY_BUILDER, COLLECTORS)
                .addStatement("final $T tupleType = rte.tupleTypeFactory.typeFor($L)", TUPLE_TYPE, dataTypes);

        for(FieldSignatureInfo x: fieldInfos) {
            builder.addParameter(x.typeName, x.fieldName, Modifier.FINAL)
                    .addStatement("final Object $L_encoded = meta.$L.encodeFromJava($L)", x.fieldName, x.fieldName, x.fieldName);
        }


        builder.addStatement("boundValues.add($L.Tuple$L.of($L))", Tuple.class.getPackage().getName(), fieldInfos.size(), params)
                .addStatement("encodedValues.add(tupleType.newValue($L))", encodedValues)
                .returns(nextType);

        return builder.addStatement("return new $T(where)", nextType).build();
    }

    private static MethodSpec buildSymetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos) {
        final String methodName = fieldInfos
                .stream()
                .map(x -> x.fieldName)
                .reduce((a, b) -> a + "_And_" + b)
                .get() + "_" + upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);

        StringJoiner paramsJoinerRelation1AsString = new StringJoiner(",");
        StringJoiner paramsJoinerRelation2AsString = new StringJoiner(",");


        fieldInfos
                .stream()
                .map(x -> x.cqlColum)
                .forEach(x -> {
                    paramsJoinerRelation1AsString.add("\"" + x + "\"");
                    paramsJoinerRelation2AsString.add("\"" + x + "\"");
                });

        final String paramsRelation1AsString = paramsJoinerRelation1AsString.toString();
        final String paramsRelation2AsString = paramsJoinerRelation2AsString.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        paramsRelation1AsString, relation1, paramsJoinerRelation2AsString, relation2)
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
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L))", x.fieldName, relation1Param);
        }

        for(FieldSignatureInfo x: fieldInfos) {
            final String relation2Param = x.fieldName + "_" + upperCaseFirst(relation2);
            builder.addParameter(x.typeName, relation2Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation2Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L))", x.fieldName, relation2Param);
        }

        builder.returns(nextType);

        return builder.addStatement("return new $T(where)", nextType).build();
    }

    private static MethodSpec buildAsymetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos1, List<FieldSignatureInfo> fieldInfos2) {
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
                .map(x -> x.cqlColum)
                .forEach(x -> paramsJoinerRelation1AsString.add("\"" + x + "\""));

        fieldInfos2
                .stream()
                .map(x -> x.cqlColum)
                .forEach(x -> paramsJoinerRelation2AsString.add("\"" + x + "\""));

        final String paramsRelation1AsString = paramsJoinerRelation1AsString.toString();
        final String paramsRelation2AsString = paramsJoinerRelation2AsString.toString();

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        paramsRelation1AsString, relation1, paramsJoinerRelation2AsString, relation2)
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
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L))", x.fieldName, relation1Param);
        }

        for(FieldSignatureInfo x: fieldInfos2) {
            final String relation2Param = x.fieldName + "_" + upperCaseFirst(relation2);
            builder.addParameter(x.typeName, relation2Param, Modifier.FINAL)
                    .addStatement("boundValues.add($L)", relation2Param)
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L))", x.fieldName, relation2Param);
        }


        builder.returns(nextType);

        return builder.addStatement("return new $T(where)", nextType).build();
    }


    private static MethodSpec buildDoubleColumnRelation(String relation1, String relation2, TypeName nextType, FieldSignatureInfo fieldInfo) {
        final String methodName = fieldInfo.fieldName + "_" + upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);
        final String param1 = fieldInfo.fieldName + "_" + upperCaseFirst(relation1);
        final String param2 = fieldInfo.fieldName + "_" + upperCaseFirst(relation2);
        final String column1 = fieldInfo.cqlColum + "_" + upperCaseFirst(relation2);
        final String column2 = fieldInfo.cqlColum + "_" + upperCaseFirst(relation2);

        return MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ? AND $L $L ?</strong>",
                        fieldInfo.cqlColum, relationNameToSymbol(relation1),
                        fieldInfo.cqlColum, relationNameToSymbol(relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param1)
                .addParameter(fieldInfo.typeName, param2)
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation1, fieldInfo.cqlColum, QUERY_BUILDER, column1)
                .addStatement("where.and($T.$L($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, relation2, fieldInfo.cqlColum, QUERY_BUILDER, column2)
                .addStatement("boundValues.add($L)", param1)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldInfo.fieldName, param1)
                .addStatement("boundValues.add($L)", param2)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldInfo.fieldName, param2)
                .addStatement("return new $T(where)", nextType)
                .returns(nextType)
                .build();
    }
}

