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

package info.archinnov.achilles.internals.codegen.index.cassandra3_7;

import static info.archinnov.achilles.internals.parser.TypeUtils.DSL_PACKAGE;
import static info.archinnov.achilles.internals.parser.TypeUtils.STRING;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.annotations.SASI;
import info.archinnov.achilles.internals.codegen.index.SASISupport;
import info.archinnov.achilles.internals.codegen.index.cassandra2_2.IndexSelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;
import info.archinnov.achilles.internals.parser.context.SASIInfoContext;

public class IndexSelectWhereDSLCodeGen3_7 extends IndexSelectWhereDSLCodeGen2_2 implements SASISupport {

    @Override
    public void buildSASIIndexRelation(TypeSpec.Builder indexSelectWhereBuilder, EntityMetaCodeGen.EntityMetaSignature signature, String parentClassName, ClassSignatureInfo lastSignature, ReturnType returnType) {
        final List<IndexFieldSignatureInfo> sasiIndexCols = getIndexedColsSignatureInfo(IndexImpl.SASI, signature.fieldMetaSignatures);
        sasiIndexCols.forEach(x -> buildSASIIndexRelation(indexSelectWhereBuilder, x, parentClassName, lastSignature, returnType));
    }

    public void buildSASIIndexRelation(TypeSpec.Builder indexSelectWhereBuilder,
                                       IndexFieldSignatureInfo indexFieldInfo,
                                       String parentClassName,
                                       ClassSignatureInfo lastSignature,
                                       ReturnType returnType) {

        final String relationClassName = "Indexed_" + upperCaseFirst(indexFieldInfo.fieldName);
        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, parentClassName + "." + relationClassName);

        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(relationClassName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        if (indexFieldInfo.indexMetaSignature.simpleType.equals(STRING)) {
            final SASIInfoContext sasiInfoContext = indexFieldInfo.indexInfo.sasiInfoContext.get();
            if (sasiInfoContext.indexMode == SASI.IndexMode.PREFIX) {
                relationClassBuilder.addMethod(buildSASIStartWith(lastSignature.returnClassType, indexFieldInfo, returnType));
            } else if (sasiInfoContext.indexMode == SASI.IndexMode.CONTAINS) {
                relationClassBuilder.addMethod(buildSASIEndWith(lastSignature.returnClassType, indexFieldInfo, returnType));

                if (sasiInfoContext.analyzerClass == SASI.Analyzer.NON_TOKENIZING_ANALYZER) {
                    relationClassBuilder.addMethod(buildSASIStartWith(lastSignature.returnClassType, indexFieldInfo, returnType));
                }

                relationClassBuilder.addMethod(buildSASIContains(lastSignature.returnClassType, indexFieldInfo, returnType));
            }
            relationClassBuilder.addMethod(buildSASILike(lastSignature.returnClassType, indexFieldInfo, returnType));
            relationClassBuilder.addMethod(buildColumnRelation(EQ, lastSignature.returnClassType, indexFieldInfo, returnType));
        } else {
            relationClassBuilder.addMethod(buildColumnRelation(EQ, lastSignature.returnClassType, indexFieldInfo, returnType));
            relationClassBuilder.addMethod(buildColumnRelation(GT, lastSignature.returnClassType, indexFieldInfo, returnType));
            relationClassBuilder.addMethod(buildColumnRelation(GTE, lastSignature.returnClassType, indexFieldInfo, returnType));
            relationClassBuilder.addMethod(buildColumnRelation(LT, lastSignature.returnClassType, indexFieldInfo, returnType));
            relationClassBuilder.addMethod(buildColumnRelation(LTE, lastSignature.returnClassType, indexFieldInfo, returnType));
        }

        augmentRelationClassForWhereClause(relationClassBuilder, indexFieldInfo, lastSignature, returnType);

        indexSelectWhereBuilder.addType(relationClassBuilder.build());

        indexSelectWhereBuilder.addMethod(buildIndexedRelationMethod(indexFieldInfo.fieldName, relationClassTypeName));

    }
}
