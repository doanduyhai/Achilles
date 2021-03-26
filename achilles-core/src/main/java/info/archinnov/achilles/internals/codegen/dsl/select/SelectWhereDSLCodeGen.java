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

package info.archinnov.achilles.internals.codegen.dsl.select;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.BaseSingleColumnRestriction;
import info.archinnov.achilles.internals.codegen.dsl.MultiColumnsSliceRestrictionCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public abstract class SelectWhereDSLCodeGen extends AbstractDSLCodeGen
        implements BaseSingleColumnRestriction, MultiColumnsSliceRestrictionCodeGen {

    public abstract void augmentSelectEndClass(TypeSpec.Builder selectEndClassBuilder, ClassSignatureInfo lastSignature);

    public abstract List<TypeSpec> generateExtraWhereClasses(GlobalParsingContext context,
                                                             EntityMetaSignature signature,
                                                             List<FieldSignatureInfo> partitionKeys,
                                                             List<FieldSignatureInfo> clusteringCols);

    public abstract void augmentRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                            FieldSignatureInfo fieldSignatureInfo,
                                                            ClassSignatureInfo nextSignature,
                                                            ReturnType returnType);

//    public abstract void augmentClusteringWhereClass(TypeSpec.Builder clusteringClassBuilder,
//                                                     String rootClassName,
//                                                     List<ClassSignatureInfo> classesSignature,
//                                                     ClassSignatureInfo lastSignature);

    public List<TypeSpec> buildWhereClasses(GlobalParsingContext context, EntityMetaSignature signature) {
        SelectWhereDSLCodeGen selectWhereDSLCodeGen = context.selectWhereDSLCodeGen();

        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);


        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                WHERE_DSL_SUFFIX, END_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION, ABSTRACT_SELECT_WHERE);

        final ClassSignatureParams typedMapClassSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                WHERE_TYPED_MAP_DSL_SUFFIX, END_TYPED_MAP_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION_TYPED_MAP, ABSTRACT_SELECT_WHERE_TYPED_MAP);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesInternal(signature, selectWhereDSLCodeGen, partitionKeys, clusteringCols,
                classSignatureParams);
        final List<TypeSpec> partitionKeysWhereTypedMapClasses = buildWhereClassesInternal(signature, selectWhereDSLCodeGen, partitionKeys, clusteringCols,
                typedMapClassSignatureParams);
        partitionKeysWhereClasses.addAll(partitionKeysWhereTypedMapClasses);

        partitionKeysWhereClasses.addAll(generateExtraWhereClasses(context, signature, partitionKeys, clusteringCols));

        return partitionKeysWhereClasses;
    }

    public List<TypeSpec> buildWhereClassesInternal(EntityMetaSignature signature, SelectWhereDSLCodeGen selectWhereDSLCodeGen,
                                                    List<FieldSignatureInfo> partitionKeys, List<FieldSignatureInfo> clusteringCols,
                                                    ClassSignatureParams classSignatureParams) {

        final List<ClassSignatureInfo> classesSignature = selectWhereDSLCodeGen.buildClassesSignatureForWhereClause(signature, classSignatureParams,
                partitionKeys, clusteringCols, WhereClauseFor.NORMAL);

        final Optional<ClassSignatureInfo> firstClusteringClassSignature = classesSignature.stream()
                .filter(x -> x.columnType == ColumnType.CLUSTERING)
                .findFirst();

        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(signature.selectClassName(), true, lastSignature, classesSignature);
        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(signature, firstClusteringClassSignature, classesSignature, lastSignature);

        final TypeSpec selectEndClass = buildSelectEndClass(signature, lastSignature, firstClusteringClassSignature);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(selectEndClass);
        return partitionKeysWhereClasses;
    }

    public TypeSpec buildSelectEndClass(EntityMetaSignature signature, ClassSignatureInfo lastSignature, Optional<ClassSignatureInfo> firstClusteringClassSignature) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(lastSignature.className)
                .superclass(lastSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(SELECT_DOT_WHERE))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(lastSignature))
                .addMethod(buildGetThis(lastSignature.returnClassType));

        augmentSelectEndClass(builder, lastSignature);

        maybeBuildOrderingBy(lastSignature, firstClusteringClassSignature.map(x -> x.fieldSignatureInfo), builder);

        return builder.build();
    }

    public void maybeBuildOrderingBy(ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> fieldSignatureInfoOptional, TypeSpec.Builder builder) {
        if (fieldSignatureInfoOptional.isPresent()) {
            final FieldSignatureInfo fieldSignatureInfo = fieldSignatureInfoOptional.get();
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
                                                            boolean buildTokenFunction,
                                                            ClassSignatureInfo lastSignature,
                                                            List<ClassSignatureInfo> classesSignature) {
        final ColumnType columnType = classesSignature.get(0).columnType;
        if (columnType == ColumnType.CLUSTERING || columnType == null) {
            return new ArrayList<>();
        } else {
            final ClassSignatureInfo classSignature = classesSignature.remove(0);
            final TypeSpec.Builder builder = buildSelectWhereForPartitionKey(rootClassName, classSignature, classesSignature.get(0));
            final TypeSpec typeSpec;
            if (buildTokenFunction) {
                final List<String> partitionKeyColumns = new ArrayList<>(classesSignature
                        .stream()
                        .filter(x -> x.columnType == ColumnType.PARTITION)
                        .map(x -> x.fieldSignatureInfo.quotedCqlColumn)
                        .collect(toList()));
                partitionKeyColumns.add(0, classSignature.fieldSignatureInfo.quotedCqlColumn);
                typeSpec = augmentWithTokenValueRelationClass(rootClassName, builder, lastSignature, classSignature, partitionKeyColumns);
            } else {
                typeSpec = builder.build();
            }

            final List<TypeSpec> typeSpecs = buildWhereClassesForPartitionKeys(rootClassName, false, lastSignature, classesSignature);
            typeSpecs.add(0, typeSpec);
            return typeSpecs;
        }
    }

    public TypeSpec augmentWithTokenValueRelationClass(String rootClassName,
                                                       TypeSpec.Builder builder,
                                                       ClassSignatureInfo nextClassSignatureForTokenFunction,
                                                       ClassSignatureInfo classSignature,
                                                       List<String> partitionKeyColumns) {

        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_TOKEN);

        final TypeSpec tokenClass = TypeSpec.classBuilder(DSL_TOKEN)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildTokenValueRelation(EQ, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildTokenValueRelation(GT, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildTokenValueRelation(GTE, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildTokenValueRelation(LT, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildTokenValueRelation(LTE, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildDoubleTokenValueRelation(GT, LT, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildDoubleTokenValueRelation(GT, LTE, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildDoubleTokenValueRelation(GTE, LT, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .addMethod(buildDoubleTokenValueRelation(GTE, LTE, nextClassSignatureForTokenFunction.returnClassType, partitionKeyColumns, ReturnType.NEW))
                .build();

        final String methodName = partitionKeyColumns.stream().collect(Collectors.joining("_", "tokenValueOf_", "")).replaceAll("\"", "");

        return builder
                .addType(tokenClass)
                .addMethod(buildRelationMethod(methodName,relationClassTypeName))
                .build();

    }

    public TypeSpec.Builder buildSelectWhereForPartitionKey(String rootClassName,
                                                    ClassSignatureInfo classSignature,
                                                    ClassSignatureInfo nextSignature) {

        FieldSignatureInfo partitionInfo = classSignature.fieldSignatureInfo;

        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_RELATION);

        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, partitionInfo, ReturnType.NEW))
                .addMethod(buildColumnInVarargs(nextSignature.returnClassType, partitionInfo, ReturnType.NEW));

        augmentRelationClassForWhereClause(relationClassBuilder, partitionInfo, nextSignature, ReturnType.NEW);

        return TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(SELECT_DOT_WHERE))
                .addType(relationClassBuilder.build())
                .addMethod(buildRelationMethod(partitionInfo.fieldName, relationClassTypeName));
    }


    public List<TypeSpec> buildWhereClassesForClusteringColumns(EntityMetaSignature signature,
                                                                Optional<ClassSignatureInfo> firstClusteringClassSignature,
                                                                List<ClassSignatureInfo> classesSignature,
                                                                ClassSignatureInfo lastSignature) {
        if (classesSignature.get(0).columnType == null) {
            return new ArrayList<>();
        } else {
            final List<ClassSignatureInfo> copyClassesSignature = new ArrayList<>(classesSignature);
            classesSignature.remove(0);
            final TypeSpec currentType = buildSelectWhereForClusteringColumn(signature, firstClusteringClassSignature, copyClassesSignature, lastSignature).build();
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(signature, firstClusteringClassSignature, classesSignature, lastSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    public TypeSpec.Builder buildSelectWhereForClusteringColumn(EntityMetaSignature signature,
                                                        Optional<ClassSignatureInfo> firstClusteringClassSignature,
                                                        List<ClassSignatureInfo> classesSignature,
                                                        ClassSignatureInfo lastSignature) {

        final String rootClassName = signature.selectClassName();
        final ClassSignatureInfo classSignature = classesSignature.get(0);
        final ClassSignatureInfo nextSignature = classesSignature.get(1);
        final FieldSignatureInfo clusteringColumnInfo = classSignature.fieldSignatureInfo;
        final TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_RELATION);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(SELECT_DOT_WHERE))
                .addMethod(buildGetThis(classSignature.returnClassType))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(classSignature));

//        augmentClusteringWhereClass(builder, rootClassName, classesSignature, lastSignature);

        TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, clusteringColumnInfo, ReturnType.NEW));

        addSingleColumnSliceRestrictions(relationClassBuilder, clusteringColumnInfo, nextSignature, lastSignature, ReturnType.NEW);

        augmentRelationClassForWhereClause(relationClassBuilder, clusteringColumnInfo, nextSignature, ReturnType.NEW);

        builder.addType(relationClassBuilder.build());
        builder.addMethod(buildRelationMethod(clusteringColumnInfo.fieldName, relationClassTypeName));

        String parentClassName = signature.selectClassName() + "." + classesSignature.get(0).className;

        final List<ClassSignatureInfo> classesSignatureCopy = new ArrayList<>(classesSignature);
        classesSignatureCopy.remove(lastSignature);

        addMultipleColumnsSliceRestrictions(builder, parentClassName,
                classesSignatureCopy.stream().map(x -> x.fieldSignatureInfo).collect(toList()),
                lastSignature, ReturnType.NEW);

        maybeBuildOrderingBy(classSignature, firstClusteringClassSignature.map(x -> x.fieldSignatureInfo), builder);

        return builder;
    }
}

