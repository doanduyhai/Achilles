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

package info.archinnov.achilles.internals.codegen.dsl.select;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;

public abstract class SelectWhereDSLCodeGen extends AbstractDSLCodeGen {

    public List<TypeSpec> buildWhereClasses(EntityMetaSignature signature, SelectWhereDSLCodeGen selectWhereDSLCodeGen) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);


        final Optional<FieldSignatureInfo> firstClustering = clusteringCols.stream().limit(1).findFirst();

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                SELECT_WHERE_DSL_SUFFIX, SELECT_END_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION, ABSTRACT_SELECT_WHERE);

        final List<ClassSignatureInfo> classesSignature =
                selectWhereDSLCodeGen.buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, clusteringCols,
                        WhereClauseFor.NORMAL);

        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);
        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(signature.selectClassName(), partitionKeys, classesSignature);
        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(signature.selectClassName(), signature, firstClustering,
                clusteringCols, classesSignature, lastSignature);

        final TypeSpec selectEndClass = buildSelectEndClass(signature, lastSignature, firstClustering);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(selectEndClass);
        return partitionKeysWhereClasses;
    }

    public TypeSpec buildSelectEndClass(EntityMetaSignature signature, ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> firstClustering) {

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
                .addMethod(buildGetThis(lastSignature.returnClassType));

        maybeBuildOrderingBy(lastSignature, firstClustering, builder);

        return builder.build();
    }

    public void maybeBuildOrderingBy(ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> firstClustering, TypeSpec.Builder builder) {
        if (firstClustering.isPresent()) {

            final FieldSignatureInfo fieldSignatureInfo = firstClustering.get();
            final MethodSpec orderByAsc = MethodSpec
                    .methodBuilder("orderBy" + upperCaseFirst(fieldSignatureInfo.fieldName) + "Ascending")
                    .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>ORDER BY $L ASC</strong>", fieldSignatureInfo.cqlColumn)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .returns(lastSignature.returnClassType)
                    .addStatement("where.orderBy($T.asc($S))", QUERY_BUILDER, fieldSignatureInfo.cqlColumn)
                    .addStatement("return this")
                    .build();

            final MethodSpec orderByDesc = MethodSpec
                    .methodBuilder("orderBy" + upperCaseFirst(fieldSignatureInfo.fieldName) + "Descending")
                    .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>ORDER BY $L DESC</strong>", fieldSignatureInfo.cqlColumn)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .returns(lastSignature.returnClassType)
                    .addStatement("where.orderBy($T.desc($S))", QUERY_BUILDER, fieldSignatureInfo.cqlColumn)
                    .addStatement("return this")
                    .build();

            builder.addMethod(orderByAsc).addMethod(orderByDesc);
        }
    }

    public MethodSpec buildLimit(ClassSignatureInfo lastSignature) {
        return MethodSpec.methodBuilder("limit")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>LIMIT :limit</strong>")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT.box(), "limit", Modifier.FINAL)
                .returns(lastSignature.returnClassType)
                .addStatement("where.limit($T.bindMarker($S))", QUERY_BUILDER, "lim")
                .addStatement("boundValues.add($N)", "limit")
                .addStatement("encodedValues.add($N)", "limit")
                .addStatement("return this")
                .build();
    }


    public List<TypeSpec> buildWhereClassesForPartitionKeys(String rootClassName,
                                                            List<FieldSignatureInfo> partitionKeys,
                                                            List<ClassSignatureInfo> classesSignature) {
        if (partitionKeys.isEmpty()) {
            return new ArrayList<>();
        } else {
            final FieldSignatureInfo partitionKeyInfo = partitionKeys.remove(0);
            final ClassSignatureInfo classSignature = classesSignature.remove(0);
            final TypeSpec typeSpec = buildSelectWhereForPartitionKey(rootClassName, partitionKeyInfo, classSignature, classesSignature.get(0));
            final List<TypeSpec> typeSpecs = buildWhereClassesForPartitionKeys(rootClassName, partitionKeys, classesSignature);
            typeSpecs.add(0, typeSpec);
            return typeSpecs;
        }
    }

    public TypeSpec buildSelectWhereForPartitionKey(String rootClassName,
                                                    FieldSignatureInfo partitionInfo,
                                                    ClassSignatureInfo classSignature,
                                                    ClassSignatureInfo nextSignature) {

        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_RELATION);

        final TypeSpec relationClass = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, partitionInfo))
                .addMethod(buildColumnInVarargs(nextSignature.returnClassType, partitionInfo))
                .build();

        return TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_WHERE))
                .addType(relationClass)
                .addMethod(buildRelationMethod(partitionInfo.fieldName, relationClassTypeName))
                .build();
    }


    public List<TypeSpec> buildWhereClassesForClusteringColumns(String rootClassName,
                                                                EntityMetaSignature signature,
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
            final TypeSpec currentType = buildSelectWhereForClusteringColumn(rootClassName, signature, firstClusteringCol, copyClusteringCols, copyClassesSignature, lastSignature);
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(rootClassName, signature, firstClusteringCol, clusteringCols, classesSignature, lastSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    public TypeSpec buildSelectWhereForClusteringColumn(String rootClassName,
                                                        EntityMetaSignature signature,
                                                        Optional<FieldSignatureInfo> firstClusteringCol,
                                                        List<FieldSignatureInfo> clusteringCols,
                                                        List<ClassSignatureInfo> classesSignature,
                                                        ClassSignatureInfo lastSignature) {

        final ClassSignatureInfo classSignature = classesSignature.get(0);
        final ClassSignatureInfo nextSignature = classesSignature.get(1);
        final FieldSignatureInfo clusteringColumnInfo = clusteringCols.get(0);
        final TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_RELATION);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_WHERE))
                .addMethod(buildGetThis(classSignature.returnClassType))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(classSignature));

        TypeSpec relationClass = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildColumnInVarargs(nextSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(GT, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(GTE, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(LT, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildColumnRelation(LTE, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GT, LT, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GT, LTE, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GTE, LT, lastSignature.returnClassType, clusteringColumnInfo))
                .addMethod(buildDoubleColumnRelation(GTE, LTE, lastSignature.returnClassType, clusteringColumnInfo))
                .build();

        builder.addType(relationClass);
        builder.addMethod(buildRelationMethod(clusteringColumnInfo.fieldName, relationClassTypeName));

        // Tuple notation (col1, col2, ..., colN) < (:col1, :col2, ..., :colN)
        for (int i = 2; i <= clusteringCols.size(); i++) {
            final List<FieldSignatureInfo> fieldInfos = clusteringCols.stream().limit(i).collect(toList());
            final List<FieldSignatureInfo> fieldInfosMinusOne = clusteringCols.stream().limit(i - 1).collect(toList());

            String multiRelationName = fieldInfos
                    .stream().map(x -> x.fieldName).reduce((a, b) -> a + "_And_" + b)
                    .get();

            TypeName multiRelationClassTypeName = ClassName.get(DSL_PACKAGE, signature.selectClassName()
                            + "." + classSignature.className
                            + "." + multiRelationName + DSL_RELATION_SUFFIX);

            TypeSpec multiRelationClass = TypeSpec.classBuilder(multiRelationName + DSL_RELATION_SUFFIX)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addMethod(buildTuplesColumnRelation(GT, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(GTE, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(LT, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildTuplesColumnRelation(LTE, lastSignature.returnClassType, fieldInfos))

                    .addMethod(buildSymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildSymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildSymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfos))
                    .addMethod(buildSymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfos))

                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfos, fieldInfosMinusOne))

                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos))
                    .addMethod(buildAsymmetricColumnDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfosMinusOne, fieldInfos))
                    .build();

            builder.addType(multiRelationClass);
            builder.addMethod(buildRelationMethod(multiRelationName, multiRelationClassTypeName));
        }

        maybeBuildOrderingBy(classSignature, firstClusteringCol, builder);

        return builder.build();
    }

    public MethodSpec buildTuplesColumnRelation(String relation, TypeName nextType, List<FieldSignatureInfo> fieldInfos) {
        final String methodName = upperCaseFirst(relation);

        StringJoiner paramsJoiner = new StringJoiner(",");
        StringJoiner dataTypeJoiner = new StringJoiner(",");
        fieldInfos
                .stream()
                .map(x -> x.quotedCqlColumn)
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
                    .addStatement("encodedValues.add(meta.$L.encodeFromJava($L))", x.fieldName, x.fieldName);
        }

        builder.returns(nextType);

        return builder.addStatement("return new $T(where)", nextType).build();
    }

    public MethodSpec buildSymmetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos) {
        final String methodName = upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);

        StringJoiner paramsJoinerRelation1AsString = new StringJoiner(",");
        StringJoiner paramsJoinerRelation2AsString = new StringJoiner(",");


        fieldInfos
                .stream()
                .map(x -> x.quotedCqlColumn)
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

    public MethodSpec buildAsymmetricColumnDoubleRelation(String relation1, String relation2, TypeName nextType, List<FieldSignatureInfo> fieldInfos1, List<FieldSignatureInfo> fieldInfos2) {
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
                .map(x -> x.quotedCqlColumn)
                .forEach(x -> paramsJoinerRelation1AsString.add("\"" + x + "\""));

        fieldInfos2
                .stream()
                .map(x -> x.quotedCqlColumn)
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


    public MethodSpec buildDoubleColumnRelation(String relation1, String relation2, TypeName nextType, FieldSignatureInfo fieldInfo) {
        final String methodName = upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2);
        final String param1 = fieldInfo.fieldName + "_" + upperCaseFirst(relation1);
        final String param2 = fieldInfo.fieldName + "_" + upperCaseFirst(relation2);
        final String column1 = fieldInfo.cqlColumn + "_" + upperCaseFirst(relation2);
        final String column2 = fieldInfo.cqlColumn + "_" + upperCaseFirst(relation2);

        return MethodSpec.methodBuilder(methodName)
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
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldInfo.fieldName, param1)
                .addStatement("boundValues.add($L)", param2)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldInfo.fieldName, param2)
                .addStatement("return new $T(where)", nextType)
                .returns(nextType)
                .build();
    }
}

