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

package info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_10;

import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.CLUSTERING;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.PARTITION;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.type.tuples.Tuple2;

public class SelectDSLCodeGen3_10 extends SelectDSLCodeGen2_2 {

    @Override
    public TypeSpec.Builder buildSelectFrom(EntityMetaSignature signature, String firstPartitionKey) {
        final TypeSpec.Builder builder = super.buildSelectFrom(signature, firstPartitionKey);
        addGroupBy(signature, builder, FROM_DSL_SUFFIX);

        return builder;
    }

    @Override
    public TypeSpec.Builder buildSelectFromTypedMap(EntityMetaSignature signature, String firstPartitionKey) {
        final TypeSpec.Builder builder = super.buildSelectFromTypedMap(signature, firstPartitionKey);
        addGroupBy(signature, builder, FROM_TYPED_MAP_DSL_SUFFIX);
        return builder;
    }

    @Override
    public TypeSpec.Builder buildSelectFromJSON(EntityMetaSignature signature, String className, TypeName selectWhereJSONTypeName, TypeName selectEndJSONTypeName) {
        final TypeSpec.Builder builder = super.buildSelectFromJSON(signature, className, selectWhereJSONTypeName, selectEndJSONTypeName);
        addGroupBy(signature, builder, FROM_JSON_DSL_SUFFIX);
        return builder;
    }

    private void addGroupBy(EntityMetaSignature signature, TypeSpec.Builder builder, String DSLSuffix) {
        final TypeName selectEndTypeName = ClassName.get(DSL_PACKAGE, signature.selectEndReturnType());

        final TypeName groupByClassTypeName = ClassName.get(DSL_PACKAGE, signature.selectClassName()
                + "." + DSLSuffix
                + "." + DSL_GROUP_BY);


        final TypeSpec.Builder groupByClassBuilder = TypeSpec.classBuilder(DSL_GROUP_BY)
                .addAnnotation(Deprecated.class)
                .addJavadoc("Class for internal use only !!!")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        final String partitionCQLColumnsForJavaDoc = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> x.context.quotedCqlColumn)
                .collect(Collectors.joining(", "));

        final String partitionCQLColumns = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> x.context.quotedCqlColumn)
                .map(quotedColumn -> "QueryBuilder.column(\"" + quotedColumn.replaceAll("\"", "\\\\\"") + "\")")
                .collect(Collectors.joining(", "));

        final String groupByAllPartitionColumnsMethod = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> x.context.fieldName)
                .collect(Collectors.joining("_"));

        groupByClassBuilder.addMethod(MethodSpec.methodBuilder(groupByAllPartitionColumnsMethod)
                .addJavadoc("SELECT ... FROM ... WHERE ... GROUP BY $L", partitionCQLColumnsForJavaDoc)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.groupBy($L)", partitionCQLColumns)
                .addStatement("return new $T(where, cassandraOptions)", selectEndTypeName)
                .returns(selectEndTypeName)
                .build());

        final List<Tuple2<String, String>> clusteringColumnsInfo = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple2.of(x.context.fieldName, x.context.quotedCqlColumn))
                .collect(Collectors.toList());

        for (int i=0; i<clusteringColumnsInfo.size(); i++) {

            final List<Tuple2<String, String>> subList = clusteringColumnsInfo.subList(0, clusteringColumnsInfo.size()-i);

            final String methodName = subList
                    .stream()
                    .map(Tuple2::_1)
                    .collect(Collectors.joining("_", groupByAllPartitionColumnsMethod + "_", ""));

            final String groupByColumnsForJavaDoc = subList
                    .stream()
                    .map(Tuple2::_2)
                    .collect(Collectors.joining(", ", partitionCQLColumnsForJavaDoc + ", ", ""));

            final String groupByColumns = subList
                    .stream()
                    .map(Tuple2::_2)
                    .map(quotedColumn -> "QueryBuilder.column(\"" + quotedColumn.replaceAll("\"", "\\\\\"") + "\")")
                    .collect(Collectors.joining(", ", partitionCQLColumns + ", ", ""));

            groupByClassBuilder.addMethod(MethodSpec.methodBuilder(methodName)
                    .addJavadoc("SELECT ... FROM ... WHERE ... GROUP BY $L", groupByColumnsForJavaDoc)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addStatement("where.groupBy($L)", groupByColumns)
                    .addStatement("return new $T(where, cassandraOptions)", selectEndTypeName)
                    .returns(selectEndTypeName)
                    .build());
        }

        builder
                .addMethod(MethodSpec.methodBuilder("groupBy")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .returns(groupByClassTypeName)
                        .addStatement("return new $T()", groupByClassTypeName)
                        .build())
                .addType(groupByClassBuilder.build());
    }
}
