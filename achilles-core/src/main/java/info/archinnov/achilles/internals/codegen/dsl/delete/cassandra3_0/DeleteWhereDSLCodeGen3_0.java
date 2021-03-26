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

package info.archinnov.achilles.internals.codegen.dsl.delete.cassandra3_0;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.MultiColumnsSliceRestrictionCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_2.DeleteWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;

public class DeleteWhereDSLCodeGen3_0 extends DeleteWhereDSLCodeGen2_2
        implements MultiColumnsSliceRestrictionCodeGen {

    /**
     * Add slice delete, every relation class for clustering columns should implement AbstractDeleteEnd class
     * @param signature
     * @return
     */
    @Override
    public List<TypeSpec> buildWhereClasses(EntityMetaSignature signature) {
        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);

        final ClassSignatureParams classSignatureParams = ClassSignatureParams.of(DELETE_DSL_SUFFIX,
                WHERE_DSL_SUFFIX, END_DSL_SUFFIX,
                ABSTRACT_DELETE_WHERE_PARTITION, ABSTRACT_DELETE_END);

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

    /**
     * Generate extra method to extends the AbstractDeleteEnd class
     * @param signature
     * @param clusteringCols
     * @param classesSignature
     * @param lastSignature
     * @return
     */
    @Override
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

        final TypeSpec.Builder whereClassBuilder = TypeSpec.classBuilder(classSignature.className)
                .superclass(classSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructorWithOptions(DELETE_DOT_WHERE))

                .addMethod(buildGetThis(classSignature.returnClassType))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal());

        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(DSL_RELATION)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildColumnRelation(EQ, nextSignature.returnClassType, clusteringColumnInfo, ReturnType.NEW));

        augmentClusteringColRelationClassForWhereClause(relationClassBuilder, clusteringColumnInfo, nextSignature, lastSignature);

        whereClassBuilder
                .addMethod(buildRelationMethod(clusteringColumnInfo.fieldName, relationClassTypeName))
                .addType(relationClassBuilder.build());

        augmentWhereClass(whereClassBuilder, signature, clusteringCols, classesSignature, lastSignature);

        return whereClassBuilder.build();
    }

    @Override
    public void augmentClusteringColRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder, FieldSignatureInfo fieldInfo,
                                                                ClassSignatureInfo nextSignature, ClassSignatureInfo lastSignature) {
        super.augmentClusteringColRelationClassForWhereClause(relationClassBuilder, fieldInfo, nextSignature, lastSignature);
        addSingleColumnSliceRestrictions(relationClassBuilder, fieldInfo, nextSignature, lastSignature, ReturnType.NEW);
    }

    @Override
    public void augmentWhereClass(TypeSpec.Builder whereClassBuilder,
                                  EntityMetaSignature signature,
                                  List<FieldSignatureInfo> clusteringCols,
                                  List<ClassSignatureInfo> classesSignature,
                                  ClassSignatureInfo lastSignature) {
        String parentClassName = signature.deleteClassName() + "." + classesSignature.get(0).className;
        addMultipleColumnsSliceRestrictions(whereClassBuilder, parentClassName, clusteringCols, lastSignature, ReturnType.NEW);
    }
}
