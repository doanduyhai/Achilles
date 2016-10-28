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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.lang.model.element.Modifier;


import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.BaseSingleColumnRestriction;
import info.archinnov.achilles.internals.codegen.dsl.MultiColumnsSliceRestrictionCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
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

    public List<TypeSpec> buildWhereClasses(GlobalParsingContext context, EntityMetaSignature signature) {
        SelectWhereDSLCodeGen selectWhereDSLCodeGen = context.selectWhereDSLCodeGen();

        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);


        final Optional<FieldSignatureInfo> firstClustering = clusteringCols.stream().limit(1).findFirst();

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                SELECT_WHERE_DSL_SUFFIX, SELECT_END_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION, ABSTRACT_SELECT_WHERE);

        final ClassSignatureParams typedMapClassSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                SELECT_WHERE_TYPED_MAP_DSL_SUFFIX, SELECT_END_TYPED_MAP_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION_TYPED_MAP, ABSTRACT_SELECT_WHERE_TYPED_MAP);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesInternal(signature, selectWhereDSLCodeGen, partitionKeys, clusteringCols,
                firstClustering, classSignatureParams);
        final List<TypeSpec> partitionKeysWhereTypedMapClasses = buildWhereClassesInternal(signature, selectWhereDSLCodeGen, partitionKeys, clusteringCols,
                firstClustering, typedMapClassSignatureParams);
        partitionKeysWhereClasses.addAll(partitionKeysWhereTypedMapClasses);

        partitionKeysWhereClasses.addAll(generateExtraWhereClasses(context, signature, partitionKeys, clusteringCols));

        return partitionKeysWhereClasses;
    }

    public List<TypeSpec> buildWhereClassesInternal(EntityMetaSignature signature, SelectWhereDSLCodeGen selectWhereDSLCodeGen,
                                                    List<FieldSignatureInfo> partitionKeys, List<FieldSignatureInfo> clusteringCols,
                                                    Optional<FieldSignatureInfo> firstClustering, ClassSignatureParams classSignatureParams) {

        List<FieldSignatureInfo> partitionKeysCopy = new ArrayList<>(partitionKeys);
        List<FieldSignatureInfo> clusteringColsCopy = new ArrayList<>(clusteringCols);

        final List<ClassSignatureInfo> classesSignature = selectWhereDSLCodeGen.buildClassesSignatureForWhereClause(signature, classSignatureParams,
                partitionKeysCopy, clusteringColsCopy, WhereClauseFor.NORMAL);

        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);
        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(signature.selectClassName(), partitionKeysCopy, classesSignature);
        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(signature, firstClustering,
                clusteringColsCopy, classesSignature, lastSignature);

        final TypeSpec selectEndClass = buildSelectEndClass(signature, lastSignature, firstClustering);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(selectEndClass);
        return partitionKeysWhereClasses;
    }

    public TypeSpec buildSelectEndClass(EntityMetaSignature signature, ClassSignatureInfo lastSignature, Optional<FieldSignatureInfo> firstClustering) {

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
                .addMethod(buildRelationMethod(partitionInfo.fieldName, relationClassTypeName))
                .build();
    }


    public List<TypeSpec> buildWhereClassesForClusteringColumns(EntityMetaSignature signature,
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
            final TypeSpec currentType = buildSelectWhereForClusteringColumn(signature, firstClusteringCol,
                    copyClusteringCols, copyClassesSignature, lastSignature);
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(signature, firstClusteringCol,
                    clusteringCols, classesSignature, lastSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    public TypeSpec buildSelectWhereForClusteringColumn(EntityMetaSignature signature,
                                                        Optional<FieldSignatureInfo> firstClusteringCol,
                                                        List<FieldSignatureInfo> clusteringCols,
                                                        List<ClassSignatureInfo> classesSignature,
                                                        ClassSignatureInfo lastSignature) {

        final String rootClassName = signature.selectClassName();
        final ClassSignatureInfo classSignature = classesSignature.get(0);
        final ClassSignatureInfo nextSignature = classesSignature.get(1);
        final FieldSignatureInfo clusteringColumnInfo = clusteringCols.get(0);
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

        TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, clusteringColumnInfo, ReturnType.NEW));

        addSingleColumnSliceRestrictions(relationClassBuilder, clusteringColumnInfo, nextSignature, lastSignature, ReturnType.NEW);

        augmentRelationClassForWhereClause(relationClassBuilder, clusteringColumnInfo, nextSignature, ReturnType.NEW);

        builder.addType(relationClassBuilder.build());
        builder.addMethod(buildRelationMethod(clusteringColumnInfo.fieldName, relationClassTypeName));

        String parentClassName = signature.selectClassName() + "." + classesSignature.get(0).className;

        addMultipleColumnsSliceRestrictions(builder, parentClassName, clusteringCols, lastSignature, ReturnType.NEW);

        maybeBuildOrderingBy(classSignature, firstClusteringCol, builder);

        return builder.build();
    }
}

