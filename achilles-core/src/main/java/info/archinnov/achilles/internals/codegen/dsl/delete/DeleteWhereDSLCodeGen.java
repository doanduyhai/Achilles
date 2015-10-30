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

package info.archinnov.achilles.internals.codegen.dsl.delete;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;


public class DeleteWhereDSLCodeGen extends AbstractDSLCodeGen {

    public static List<TypeSpec> buildWhereClasses(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.parsingResults);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.parsingResults);

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(DELETE_WHERE_DSL_SUFFIX,
                DELETE_END_DSL_SUFFIX, ABSTRACT_DELETE_WHERE_PARTITION, ABSTRACT_DELETE_WHERE, ABSTRACT_DELETE_END);

        final List<ClassSignatureInfo> classesSignature =
                buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, clusteringCols,
                        WhereClauseFor.NORMAL);

        boolean hasCounter = hasCounter(signature);
        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(partitionKeys,
                classesSignature, clusteringCols.size() > 0);

        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(clusteringCols,
                classesSignature);

        final TypeSpec deleteEndClass = buildDeleteEndClass(signature, lastSignature, hasCounter);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(deleteEndClass);
        return partitionKeysWhereClasses;
    }

    public static List<TypeSpec> buildWhereClassesForStatic(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.parsingResults);

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(DELETE_STATIC_WHERE_DSL_SUFFIX,
                DELETE_STATIC_END_DSL_SUFFIX, ABSTRACT_DELETE_WHERE_PARTITION, ABSTRACT_DELETE_WHERE, ABSTRACT_DELETE_END);

        final List<ClassSignatureInfo> classesSignature =
                buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, Arrays.asList(),
                        WhereClauseFor.STATIC);

        boolean hasCounter = hasCounter(signature);
        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(partitionKeys, classesSignature, false);

        final TypeSpec deleteEndClass = buildDeleteEndClass(signature, lastSignature, hasCounter);

        partitionKeysWhereClasses.add(deleteEndClass);
        return partitionKeysWhereClasses;
    }


    private static TypeSpec buildDeleteEndClass(EntityMetaSignature signature,
                                                ClassSignatureInfo lastSignature,
                                                boolean hasCounter) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(lastSignature.className)
                .superclass(lastSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(DELETE_WHERE))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildGetThis(lastSignature.classType));

        buildLWtConditionMethods(signature, lastSignature, hasCounter, builder);

        return builder.build();
    }

    private static List<TypeSpec> buildWhereClassesForPartitionKeys(List<FieldSignatureInfo> partitionKeys,
                                                                    List<ClassSignatureInfo> classesSignature,
                                                                    boolean hasClusterings) {
        if (partitionKeys.isEmpty()) {
            return new ArrayList<>();
        } else {
            final FieldSignatureInfo partitionKeyInfo = partitionKeys.get(0);
            final ClassSignatureInfo currentSignature = classesSignature.get(0);
            final ClassSignatureInfo nextSignature = classesSignature.get(1);

            partitionKeys.remove(0);
            classesSignature.remove(0);

            final TypeSpec typeSpec = buildDeleteWhereForPartitionKey(partitionKeyInfo, currentSignature,
                    nextSignature, hasClusterings);

            final List<TypeSpec> typeSpecs = buildWhereClassesForPartitionKeys(partitionKeys,
                    classesSignature, hasClusterings);

            typeSpecs.add(0, typeSpec);
            return typeSpecs;
        }
    }

    private static TypeSpec buildDeleteWhereForPartitionKey(FieldSignatureInfo partitionInfo,
                                                            ClassSignatureInfo classSignature,
                                                            ClassSignatureInfo nextSignature,
                                                            boolean hasClusterings) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(DELETE_WHERE))
                .addMethod(buildColumnRelation(EQ, nextSignature.classType, partitionInfo));

        if (!hasClusterings) {
            builder.addMethod(buildColumnInVarargs(nextSignature.classType, partitionInfo));
        }

        return builder.build();
    }


    private static List<TypeSpec> buildWhereClassesForClusteringColumns(List<FieldSignatureInfo> clusteringCols,
                                                                        List<ClassSignatureInfo> classesSignature) {
        if (clusteringCols.isEmpty()) {
            return new ArrayList<>();
        } else {
            final ClassSignatureInfo classSignature = classesSignature.get(0);
            final ClassSignatureInfo nextSignature = classesSignature.get(1);
            final FieldSignatureInfo clusteringColumnInfo = clusteringCols.get(0);
            clusteringCols.remove(0);
            classesSignature.remove(0);
            final TypeSpec currentType = buildDeleteWhereForClusteringColumn(clusteringColumnInfo, classSignature,
                    nextSignature);
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(clusteringCols, classesSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    private static TypeSpec buildDeleteWhereForClusteringColumn(FieldSignatureInfo clusteringColumnInfo,
                                                                ClassSignatureInfo classSignature,
                                                                ClassSignatureInfo nextSignature) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(DELETE_WHERE))
                .addMethod(buildColumnRelation(EQ, nextSignature.classType, clusteringColumnInfo));

        return builder.build();
    }
}

