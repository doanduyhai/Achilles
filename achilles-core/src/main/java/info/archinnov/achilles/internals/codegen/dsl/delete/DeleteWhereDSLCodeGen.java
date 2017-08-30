/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.BaseSingleColumnRestriction;
import info.archinnov.achilles.internals.codegen.dsl.LWTConditionsCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;


public abstract class DeleteWhereDSLCodeGen extends AbstractDSLCodeGen
        implements BaseSingleColumnRestriction, LWTConditionsCodeGen {

    public abstract void augmentPartitionKeyRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                            FieldSignatureInfo fieldSignatureInfo,
                                                            ClassSignatureInfo nextSignature);

    public abstract void augmentClusteringColRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                                        FieldSignatureInfo fieldSignatureInfo,
                                                                        ClassSignatureInfo nextSignature,
                                                                        ClassSignatureInfo lastSignature);

    public abstract void augmentWhereClass(TypeSpec.Builder whereClassBuilder,
                                           EntityMetaSignature signature,
                                           List<FieldSignatureInfo> clusteringCols,
                                           List<ClassSignatureInfo> classesSignature,
                                           ClassSignatureInfo lastSignature);

    public List<TypeSpec> buildWhereClasses(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(DELETE_DSL_SUFFIX,
                WHERE_DSL_SUFFIX, END_DSL_SUFFIX,
                ABSTRACT_DELETE_WHERE_PARTITION, ABSTRACT_DELETE_WHERE, ABSTRACT_DELETE_END);

        final List<ClassSignatureInfo> classesSignature =
                buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, clusteringCols,
                        WhereClauseFor.NORMAL);

        boolean hasCounter = hasCounter(signature);
        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(signature.deleteClassName(),
                partitionKeys, classesSignature, clusteringCols.size() > 0);

        final List<TypeSpec> clusteringColsWhereClasses = buildWhereClassesForClusteringColumns(signature,
                clusteringCols, classesSignature, lastSignature);

        final TypeSpec deleteEndClass = buildDeleteEndClass(signature, lastSignature, hasCounter);

        partitionKeysWhereClasses.addAll(clusteringColsWhereClasses);
        partitionKeysWhereClasses.add(deleteEndClass);
        return partitionKeysWhereClasses;
    }

    public List<TypeSpec> buildWhereClassesForStatic(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(DELETE_STATIC_DSL_SUFFIX,
                WHERE_DSL_SUFFIX, END_DSL_SUFFIX,
                ABSTRACT_DELETE_WHERE_PARTITION, ABSTRACT_DELETE_WHERE, ABSTRACT_DELETE_END);

        final List<ClassSignatureInfo> classesSignature =
                buildClassesSignatureForWhereClause(signature, classSignatureParams, partitionKeys, Arrays.asList(),
                        WhereClauseFor.STATIC);

        boolean hasCounter = hasCounter(signature);
        final ClassSignatureInfo lastSignature = classesSignature.get(classesSignature.size() - 1);

        final List<TypeSpec> partitionKeysWhereClasses = buildWhereClassesForPartitionKeys(signature.deleteStaticClassName(),
                partitionKeys, classesSignature, false);

        final TypeSpec deleteEndClass = buildDeleteEndClass(signature, lastSignature, hasCounter);

        partitionKeysWhereClasses.add(deleteEndClass);
        return partitionKeysWhereClasses;
    }


    public TypeSpec buildDeleteEndClass(EntityMetaSignature signature,
                                                ClassSignatureInfo lastSignature,
                                                boolean hasCounter) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(lastSignature.className)
                .superclass(lastSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(DELETE_DOT_WHERE))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildGetThis(lastSignature.returnClassType));

        buildLWtConditionMethods(signature, lastSignature.className, lastSignature, hasCounter, builder);

        return builder.build();
    }

    public List<TypeSpec> buildWhereClassesForPartitionKeys(String rootClassName,
                                                            List<FieldSignatureInfo> partitionKeys,
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

            final TypeSpec typeSpec = buildDeleteWhereForPartitionKey(rootClassName, partitionKeyInfo, currentSignature,
                    nextSignature);

            final List<TypeSpec> typeSpecs = buildWhereClassesForPartitionKeys(rootClassName, partitionKeys,
                    classesSignature, hasClusterings);

            typeSpecs.add(0, typeSpec);
            return typeSpecs;
        }
    }

    public TypeSpec buildDeleteWhereForPartitionKey(String rootClassName,
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

        augmentPartitionKeyRelationClassForWhereClause(relationClassBuilder, partitionInfo, nextSignature);

        return TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(DELETE_DOT_WHERE))
                .addType(relationClassBuilder.build())
                .addMethod(buildRelationMethod(partitionInfo.fieldName, relationClassTypeName))
                .build();

    }


    public List<TypeSpec> buildWhereClassesForClusteringColumns(EntityMetaSignature signature,
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
            final TypeSpec currentType = buildDeleteWhereForClusteringColumn(signature, copyClusteringCols, copyClassesSignature, lastSignature);
            final List<TypeSpec> typeSpecs = buildWhereClassesForClusteringColumns(signature, clusteringCols, classesSignature, lastSignature);
            typeSpecs.add(0, currentType);
            return typeSpecs;
        }
    }

    public TypeSpec buildDeleteWhereForClusteringColumn(EntityMetaSignature signature,
                                                        List<FieldSignatureInfo> clusteringCols,
                                                        List<ClassSignatureInfo> classesSignature,
                                                        ClassSignatureInfo lastSignature) {


        final ClassSignatureInfo classSignature = classesSignature.get(0);
        final ClassSignatureInfo nextSignature = classesSignature.get(1);
        final FieldSignatureInfo clusteringColumnInfo = clusteringCols.get(0);

        final String rootClassName = signature.deleteClassName();
        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_RELATION);

        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, clusteringColumnInfo, ReturnType.NEW));

        augmentClusteringColRelationClassForWhereClause(relationClassBuilder, clusteringColumnInfo, nextSignature, lastSignature);

        final TypeSpec.Builder whereClassBuilder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(DELETE_DOT_WHERE))
                .addType(relationClassBuilder.build())
                .addMethod(buildRelationMethod(clusteringColumnInfo.fieldName, relationClassTypeName));

        augmentWhereClass(whereClassBuilder, signature, clusteringCols, classesSignature, lastSignature);

        return whereClassBuilder.build();
    }
}

