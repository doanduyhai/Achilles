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

package info.archinnov.achilles.internals.codegen.index.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.index.cassandra2_1.IndexSelectWhereDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class IndexSelectWhereDSLCodeGen2_2 extends IndexSelectWhereDSLCodeGen2_1
        implements JSONFunctionCallSupport {

    @Override
    public void augmentSelectEndClass(TypeSpec.Builder selectEndClassBuilder, ClassSignatureInfo lastSignature) {
        //No Op
    }

    @Override
    public List<TypeSpec> generateExtraWhereClasses(GlobalParsingContext context,
                                                    EntityMetaCodeGen.EntityMetaSignature signature,
                                                    List<FieldSignatureInfo> partitionKeys,
                                                    List<FieldSignatureInfo> clusteringCols) {
        final ClassSignatureParams jsonClassSignatureParams = ClassSignatureParams.of(INDEX_SELECT_DSL_SUFFIX,
                WHERE_JSON_DSL_SUFFIX, END_JSON_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION_JSON, ABSTRACT_INDEX_SELECT_WHERE_JSON);
        return buildWhereClassesInternal(signature, jsonClassSignatureParams);

    }

    @Override
    public void augmentRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                   FieldSignatureInfo fieldInfo,
                                                   ClassSignatureInfo nextSignature,
                                                   ReturnType returnType) {
        // Secondary index
        final boolean isIndex = fieldInfo instanceof IndexFieldSignatureInfo;
        if (isIndex && ((IndexFieldSignatureInfo)fieldInfo).indexInfo.impl != IndexImpl.DSE_SEARCH) {
            final IndexFieldSignatureInfo indexFieldInfo = (IndexFieldSignatureInfo) fieldInfo;
            final IndexType indexType = indexFieldInfo.indexInfo.type;

            switch (indexType) {
                case COLLECTION:
                    buildJSONIndexRelationForCollection(relationClassBuilder, indexFieldInfo, nextSignature.returnClassType, returnType);
                    break;
                case MAP_KEY:
                    buildJSONIndexRelationForMapKey(relationClassBuilder, indexFieldInfo, nextSignature.returnClassType, returnType);
                    break;
                case MAP_VALUE:
                    buildJSONIndexRelationForMapValue(relationClassBuilder, indexFieldInfo, nextSignature.returnClassType, returnType);
                    break;
                case MAP_ENTRY:
                    buildJSONIndexRelationForMapEntry(relationClassBuilder, indexFieldInfo, nextSignature.returnClassType, returnType);
                    break;
                case FULL:
                case NORMAL:
                case NONE:
                    buildEqFromJSONToRelationClass(relationClassBuilder, fieldInfo, nextSignature);
                    break;
            }
        } else {
            buildEqFromJSONToRelationClass(relationClassBuilder, fieldInfo, nextSignature);
        }
    }
}
