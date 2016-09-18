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

package info.archinnov.achilles.internals.codegen.dsl.delete.cassandra3_0;

import java.util.List;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.MultiColumnsSliceRestrictionCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_2.DeleteWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;

public class DeleteWhereDSLCodeGen3_0 extends DeleteWhereDSLCodeGen2_2
        implements MultiColumnsSliceRestrictionCodeGen {

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
