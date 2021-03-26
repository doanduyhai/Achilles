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

package info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import java.util.Optional;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class SelectWhereDSLCodeGen2_2 extends SelectWhereDSLCodeGen
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
        final ClassSignatureParams jsonClassSignatureParams = ClassSignatureParams.of(SELECT_DSL_SUFFIX,
                WHERE_JSON_DSL_SUFFIX, END_JSON_DSL_SUFFIX,
                ABSTRACT_SELECT_WHERE_PARTITION_JSON, ABSTRACT_SELECT_WHERE_JSON);

        return buildWhereClassesInternal(signature, context.selectWhereDSLCodeGen(),
                partitionKeys, clusteringCols,
                jsonClassSignatureParams);

    }

    @Override
    public void augmentRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                   FieldSignatureInfo fieldInfo,
                                                   ClassSignatureInfo nextSignature, ReturnType returnType) {
        buildEqFromJSONToRelationClass(relationClassBuilder, fieldInfo, nextSignature);
    }
}
