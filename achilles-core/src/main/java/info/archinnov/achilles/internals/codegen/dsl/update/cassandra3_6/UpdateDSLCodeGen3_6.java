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

package info.archinnov.achilles.internals.codegen.dsl.update.cassandra3_6;

import java.util.Optional;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2.UpdateDSLCodeGen2_2;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;

public class UpdateDSLCodeGen3_6 extends UpdateDSLCodeGen2_2 {

    @Override
    protected void augmentUpdateRelationClass(ParentSignature parentSignature,
                                              FieldMetaSignature parsingResult, TypeName newTypeName, ReturnType returnType) {
        super.augmentUpdateRelationClass(parentSignature, parsingResult, newTypeName, returnType);
        final Optional<UDTMetaSignature> udtMetaSignature = parsingResult.udtMetaSignature;

        if (udtMetaSignature.isPresent() && !udtMetaSignature.get().isFrozen) {
            for (FieldMetaSignature udtFieldMeta : udtMetaSignature.get().fieldMetaSignatures) {

                ParentSignature nestedParentSignature = ParentSignature.of(parentSignature.aptUtils,
                        parentSignature.parentBuilder,
                        parentSignature.parentClassName,
                        Optional.of(parsingResult.context.quotedCqlColumn),
                        Optional.of(parsingResult.context.fieldName));

                buildUpdateColumnMethods(nestedParentSignature,
                        newTypeName,
                        udtFieldMeta,
                        returnType);
            }

        }
    }
}
