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

package info.archinnov.achilles.internals.codegen.index.dse_5_0_0;

import static info.archinnov.achilles.internals.parser.TypeUtils.SELECT_DOT_WHERE;

import java.util.List;
import java.util.Optional;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.index.DSESearchSupport;
import info.archinnov.achilles.internals.codegen.index.cassandra2_2.IndexSelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;

public class IndexSelectWhereDSLCodeGen_DSE_5_0_0 extends IndexSelectWhereDSLCodeGen2_2
        implements DSESearchSupport{

    @Override
    public void buildDSESearchIndexRelation(TypeSpec.Builder indexSelectWhereBuilder, EntityMetaCodeGen.EntityMetaSignature signature,
                                            String parentClassName, ClassSignatureInfo lastSignature, ReturnType returnType) {

        buildDSESearchIndexRelation(signature, indexSelectWhereBuilder,
                this::augmentRelationClassForWhereClause,
                parentClassName, lastSignature, returnType);
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

        final List<FieldSignatureInfo> partitionKeys = getPartitionKeysSignatureInfo(signature.fieldMetaSignatures);
        final List<FieldSignatureInfo> clusteringCols = getClusteringColsSignatureInfo(signature.fieldMetaSignatures);

        final List<IndexFieldSignatureInfo> nativeIndexCols = getIndexedColsSignatureInfo(IndexImpl.NATIVE, signature.fieldMetaSignatures);

        partitionKeys.forEach(x -> this.buildPartitionKeyRelation(builder, signature,x, lastSignature, classSignatureParams));

        String parentClassName = signature.indexSelectClassName() + "." + classSignatureParams.endDslSuffix;

        nativeIndexCols.forEach(x -> buildNativeIndexRelation(builder, x, parentClassName, lastSignature, ReturnType.THIS));

        buildSASIIndexRelation(builder, signature, parentClassName, lastSignature, ReturnType.THIS);
        buildDSESearchIndexRelation(builder, signature, parentClassName, lastSignature, ReturnType.THIS);

        addMultipleColumnsSliceRestrictions(builder, parentClassName, clusteringCols, lastSignature, ReturnType.THIS);

        maybeBuildOrderingBy(lastSignature, firstClustering, builder);

        return builder.build();
    }
}
