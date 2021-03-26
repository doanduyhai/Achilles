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

package info.archinnov.achilles.internals.codegen.index.cassandra2_1;

import java.util.Collections;
import java.util.List;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.index.IndexSelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class IndexSelectWhereDSLCodeGen2_1 extends IndexSelectWhereDSLCodeGen {
    @Override
    public void augmentSelectEndClass(TypeSpec.Builder selectEndClassBuilder, ClassSignatureInfo lastSignature) {
        //NO Op
    }

    @Override
    public List<TypeSpec> generateExtraWhereClasses(GlobalParsingContext context, EntityMetaCodeGen.EntityMetaSignature signature, List<FieldSignatureInfo> partitionKeys, List<FieldSignatureInfo> clusteringCols) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void augmentRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder, FieldSignatureInfo fieldSignatureInfo, ClassSignatureInfo nextSignature, ReturnType returnType) {
        //NO Op
    }

    @Override
    public void buildSASIIndexRelation(TypeSpec.Builder indexSelectWhereBuilder, EntityMetaCodeGen.EntityMetaSignature signature, String parentClassName, ClassSignatureInfo lastSignature, ReturnType returnType) {
        //NO Op
    }

    @Override
    public void buildDSESearchIndexRelation(TypeSpec.Builder indexSelectWhereBuilder, EntityMetaCodeGen.EntityMetaSignature signature, String parentClassName, ClassSignatureInfo lastSignature, ReturnType returnType) {
        //NO Op
    }
}
