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

package info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_1;

import java.util.List;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;

public class DeleteWhereDSLCodeGen2_1 extends DeleteWhereDSLCodeGen {
    @Override
    public void augmentPartitionKeyRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                               FieldSignatureInfo fieldSignatureInfo,
                                                               ClassSignatureInfo nextSignature) {
        //NO Op
    }

    @Override
    public void augmentClusteringColRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder,
                                                                FieldSignatureInfo fieldSignatureInfo,
                                                                ClassSignatureInfo nextSignature,
                                                                ClassSignatureInfo lastSignature) {
        //NO Op
    }

    @Override
    public void augmentWhereClass(TypeSpec.Builder whereClassBuilder, EntityMetaSignature signature,
                                  List<FieldSignatureInfo> clusteringCols,
                                  List<ClassSignatureInfo> classesSignature,
                                  ClassSignatureInfo lastSignature) {
        //NO Op
    }

    @Override
    public void augmentLWTConditionClass(TypeSpec.Builder conditionClassBuilder,
                                         FieldSignatureInfo fieldSignatureInfo,
                                         ClassSignatureInfo currentSignature) {
        //NO Op
    }
}
