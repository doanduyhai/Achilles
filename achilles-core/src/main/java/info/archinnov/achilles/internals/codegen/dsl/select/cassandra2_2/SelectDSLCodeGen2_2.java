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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.type.tuples.Tuple2;

public class SelectDSLCodeGen2_2 extends SelectDSLCodeGen
        implements JSONFunctionCallSupport {

    @Override
    public void augmentSelectClass(GlobalParsingContext context, EntityMetaCodeGen.EntityMetaSignature signature, TypeSpec.Builder builder) {

        TypeName selectFromJSONTypeName = ClassName.get(DSL_PACKAGE, signature.selectFromJSONReturnType());

        final String firstPartitionKey = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(TUPLE2_PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        TypeName selectWhereJSONTypeName = ClassName.get(DSL_PACKAGE, signature.selectWhereJSONReturnType(firstPartitionKey));

        TypeName selectEndJSONTypeName = ClassName.get(DSL_PACKAGE, signature.selectEndJSONReturnType());

        final String className = FROM_JSON_DSL_SUFFIX;
        builder.addType(buildSelectFromJSON(signature, className, selectWhereJSONTypeName, selectEndJSONTypeName).build());
        builder.addMethod(buildAllColumnsJSON(selectFromJSONTypeName, SELECT_DOT_WHERE, "select"));
        builder.addMethod(buildAllColumnsJSONWithSchemaProvider(selectFromJSONTypeName, SELECT_DOT_WHERE, "select"));
    }
}
