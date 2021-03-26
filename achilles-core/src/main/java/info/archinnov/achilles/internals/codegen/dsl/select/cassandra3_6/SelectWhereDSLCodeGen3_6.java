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

package info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_6;

import static info.archinnov.achilles.internals.parser.TypeUtils.QUERY_BUILDER;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectWhereDSLCodeGen2_2;

public class SelectWhereDSLCodeGen3_6 extends SelectWhereDSLCodeGen2_2 {

    @Override
    public void augmentSelectEndClass(TypeSpec.Builder selectEndClassBuilder, ClassSignatureInfo lastSignature) {

        selectEndClassBuilder.addMethod(MethodSpec.methodBuilder("perPartitionLimit")
            .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>PER PARTITION LIMIT :perPartitionLimit</strong>")
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(TypeName.INT.box(), "perPartitionLimit", Modifier.FINAL)
            .returns(lastSignature.returnClassType)
            .addStatement("where.perPartitionLimit($T.bindMarker($S))", QUERY_BUILDER, "perPartitionLimit")
            .addStatement("boundValues.add($N)", "perPartitionLimit")
            .addStatement("encodedValues.add($N)", "perPartitionLimit")
            .addStatement("return this")
            .build());
    }
}
