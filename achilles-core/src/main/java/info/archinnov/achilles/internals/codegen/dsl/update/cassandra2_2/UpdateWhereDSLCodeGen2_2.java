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

package info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.DSL_PACKAGE;
import static info.archinnov.achilles.internals.parser.TypeUtils.QUERY_BUILDER;
import static info.archinnov.achilles.internals.parser.TypeUtils.STRING;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import javax.lang.model.element.Modifier;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;

public class UpdateWhereDSLCodeGen2_2 extends UpdateWhereDSLCodeGen {

    @Override
    public void augmentRelationClassForWhereClause(TypeSpec.Builder relationClassBuilder, FieldSignatureInfo fieldInfo, ClassSignatureInfo nextSignature) {
        final String methodName = "Eq_FromJson";
        final MethodSpec fromJsonMethod = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L </strong>", fieldInfo.quotedCqlColumn, " = fromJson(?)")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.eq($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", fieldInfo.fieldName)
                .addStatement("encodedValues.add($N)", fieldInfo.fieldName)
                .returns(nextSignature.returnClassType)
                .addStatement("return new $T(where)", nextSignature.returnClassType)
                .build();

        relationClassBuilder.addMethod(fromJsonMethod);
    }

    @Override
    public void augmentLWTConditionClass(TypeSpec.Builder conditionClassBuilder, FieldSignatureInfo fieldSignatureInfo, ClassSignatureInfo currentSignature) {

        String methodName = "Eq_FromJSON";
        final String fieldName = fieldSignatureInfo.fieldName;
        final String quotedCqlColumn = fieldSignatureInfo.quotedCqlColumn;
        MethodSpec fromJsonMethod = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an ... <strong>IF $L = fromJson(?)</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add($N)", fieldName)
                .addStatement("where.onlyIf($T.eq($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, quotedCqlColumn, QUERY_BUILDER, QUERY_BUILDER, quotedCqlColumn)
                .addStatement("return $T.this", currentSignature.returnClassType)
                .returns(currentSignature.returnClassType)
                .build();

        conditionClassBuilder.addMethod(fromJsonMethod);
    }
}
