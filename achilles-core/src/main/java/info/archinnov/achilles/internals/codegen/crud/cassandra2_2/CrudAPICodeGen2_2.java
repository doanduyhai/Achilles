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

package info.archinnov.achilles.internals.codegen.crud.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.INSERT_JSON_WITH_OPTIONS;
import static info.archinnov.achilles.internals.parser.TypeUtils.STRING;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.crud.CrudAPICodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;

public class CrudAPICodeGen2_2 extends CrudAPICodeGen {

    @Override
    protected void augmentCRUDClass(EntityMetaCodeGen.EntityMetaSignature signature, TypeSpec.Builder crudClassBuilder) {
        crudClassBuilder.addMethod(MethodSpec.methodBuilder("insertJSON")
                .addJavadoc("Insert using a JSON payload\n\n")
                .addJavadoc("@json the JSON string representing an instance of $T\n", signature.entityRawClass)
                .addJavadoc("@return $T", INSERT_JSON_WITH_OPTIONS)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(STRING, "json", Modifier.FINAL)
                .addStatement("return insertJSONInternal(json, cassandraOptions)")
                .returns(INSERT_JSON_WITH_OPTIONS)
                .build());
    }
}
