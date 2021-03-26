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

package info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;

public class UpdateWhereDSLCodeGen2_2 extends UpdateWhereDSLCodeGen
        implements JSONFunctionCallSupport {

    @Override
    public void augmentPartitionKeyRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                               FieldSignatureInfo fieldInfo,
                                                               ClassSignatureInfo nextSignature) {
        buildEqFromJSONToRelationClass(relationClassBuilder, fieldInfo, nextSignature);
    }

    @Override
    public void augmentClusteringColRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                                FieldSignatureInfo fieldInfo,
                                                                ClassSignatureInfo nextSignature) {
        buildEqFromJSONToRelationClass(relationClassBuilder, fieldInfo, nextSignature);
    }

    @Override
    public void augmentLWTConditionClass(TypeSpec.Builder conditionClassBuilder,
                                         FieldSignatureInfo fieldSignatureInfo,
                                         ClassSignatureInfo currentSignature) {

        buildIfEqFromJSONToConditionClass(conditionClassBuilder, fieldSignatureInfo, currentSignature);
    }
}
