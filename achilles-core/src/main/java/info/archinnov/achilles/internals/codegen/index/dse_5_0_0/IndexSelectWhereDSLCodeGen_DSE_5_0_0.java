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

package info.archinnov.achilles.internals.codegen.index.dse_5_0_0;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import java.util.List;
import java.util.Optional;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.index.DSESearchSupport;
import info.archinnov.achilles.internals.codegen.index.cassandra2_2.IndexSelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;
import info.archinnov.achilles.internals.parser.context.DSESearchInfoContext;

public class IndexSelectWhereDSLCodeGen_DSE_5_0_0 extends IndexSelectWhereDSLCodeGen2_2
        implements DSESearchSupport{

    @Override
    public void buildDSESearchIndexRelation(TypeSpec.Builder indexSelectWhereBuilder, EntityMetaCodeGen.EntityMetaSignature signature,
                                            String parentClassName, ClassSignatureInfo lastSignature, ReturnType returnType) {
        final List<IndexFieldSignatureInfo> dseSearchColumns = getDSESearchColsSignatureInfo(signature.fieldMetaSignatures);
        dseSearchColumns.forEach(x -> buildDSESearchIndexRelation(indexSelectWhereBuilder, x, parentClassName, lastSignature, returnType));
    }

    protected void buildDSESearchIndexRelation(TypeSpec.Builder indexSelectWhereBuilder,
                                               IndexFieldSignatureInfo fieldInfo,
                                               String parentClassName,
                                               ClassSignatureInfo lastSignature,
                                               ReturnType returnType) {

        final String relationClassName = upperCaseFirst(fieldInfo.fieldName) + DSL_RELATION_SUFFIX;
        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, parentClassName + "." + relationClassName);

        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(relationClassName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        final TypeName targetType = fieldInfo.typeName;

        final DSESearchInfoContext dseSearchInfoContext = fieldInfo.indexInfo.dseSearchInfoContext.get();

        if (targetType.equals(STRING)) {
            if (dseSearchInfoContext.fullTextSearchEnabled) {
                relationClassBuilder.addMethod(buildDSETextStartWith(lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSETextEndWith(lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSETextContains(lastSignature.returnClassType, fieldInfo, returnType));
            }
            relationClassBuilder.addMethod(buildDSESingleRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
        } else if (targetType.equals(JAVA_UTIL_DATE)) {
            relationClassBuilder.addMethod(buildDSESingleDateRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleDateRelation(GT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleDateRelation(GTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleDateRelation(LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleDateRelation(LTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GT, LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GT, LTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GTE, LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GTE, LTE, lastSignature.returnClassType, fieldInfo, returnType));
        } else {
            relationClassBuilder.addMethod(buildDSESingleRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleRelation(GT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleRelation(GTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleRelation(LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSESingleRelation(LTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfo, returnType));
            relationClassBuilder.addMethod(buildDSEDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfo, returnType));
        }

        augmentRelationClassForWhereClause(relationClassBuilder, fieldInfo, lastSignature, returnType);

        indexSelectWhereBuilder.addType(relationClassBuilder.build());

        indexSelectWhereBuilder.addMethod(buildRelationMethod(fieldInfo.fieldName, relationClassTypeName));
    }

    @Override
    public TypeSpec buildSelectEndClass(EntityMetaCodeGen.EntityMetaSignature signature, ClassSignatureInfo lastSignature, ClassSignatureParams classSignatureParams) {

        final Optional<FieldSignatureInfo> firstClustering = getClusteringColsSignatureInfo(signature.fieldMetaSignatures)
                .stream()
                .limit(1)
                .findFirst();

        final TypeSpec.Builder builder = TypeSpec.classBuilder(lastSignature.className)
                .superclass(lastSignature.superType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildWhereConstructor(SELECT_DOT_WHERE))
                .addMethod(buildGetEntityClass(signature))
                .addMethod(buildGetMetaInternal(signature.entityRawClass))
                .addMethod(buildGetRte())
                .addMethod(buildGetOptions())
                .addMethod(buildGetBoundValuesInternal())
                .addMethod(buildGetEncodedBoundValuesInternal())
                .addMethod(buildLimit(lastSignature))
                .addMethod(buildGetThis(lastSignature.returnClassType));

        augmentSelectEndClass(builder, lastSignature);

        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);

        final List<IndexFieldSignatureInfo> nativeIndexCols = getIndexedColsSignatureInfo(IndexImpl.NATIVE, signature.fieldMetaSignatures);

        partitionKeys.forEach(x -> this.buildPartitionKeyRelation(builder, signature,x, lastSignature, classSignatureParams));

        String parentClassName = signature.indexSelectClassName()
                + "." + signature.className + classSignatureParams.endDslSuffix;

        nativeIndexCols.forEach(x -> buildNativeIndexRelation(builder, x, parentClassName, lastSignature, ReturnType.THIS));

        buildSASIIndexRelation(builder, signature, parentClassName, lastSignature, ReturnType.THIS);
        buildDSESearchIndexRelation(builder, signature, parentClassName, lastSignature, ReturnType.THIS);

        addMultipleColumnsSliceRestrictions(builder, parentClassName, clusteringCols, lastSignature, ReturnType.THIS);

        maybeBuildOrderingBy(lastSignature, firstClustering, builder);

        return builder.build();
    }
}
