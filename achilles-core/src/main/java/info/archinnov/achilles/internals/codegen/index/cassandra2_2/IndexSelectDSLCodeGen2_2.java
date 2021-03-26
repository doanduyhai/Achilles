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

package info.archinnov.achilles.internals.codegen.index.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.index.cassandra2_1.IndexSelectDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class IndexSelectDSLCodeGen2_2 extends IndexSelectDSLCodeGen2_1
        implements JSONFunctionCallSupport {

    @Override
    public void augmentSelectClass(GlobalParsingContext context, EntityMetaCodeGen.EntityMetaSignature signature, TypeSpec.Builder builder) {

        TypeName selectFromJSONTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectFromJSONReturnType());


        TypeName selectWhereJSONTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectWhereJSONReturnType());

        TypeName selectEndJSONTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectEndJSONReturnType());

        final String className = FROM_JSON_DSL_SUFFIX;
        builder.addType(buildSelectFromJSON(signature, className, selectWhereJSONTypeName, selectEndJSONTypeName).build());
        builder.addMethod(buildAllColumnsJSON(selectFromJSONTypeName, SELECT_DOT_WHERE, "select"));
        builder.addMethod(buildAllColumnsJSONWithSchemaProvider(selectFromJSONTypeName, SELECT_DOT_WHERE, "select"));
    }
}
