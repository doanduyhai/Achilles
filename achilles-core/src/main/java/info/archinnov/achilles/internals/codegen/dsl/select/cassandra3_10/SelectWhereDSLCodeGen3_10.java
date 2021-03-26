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

import static info.archinnov.achilles.internals.parser.TypeUtils.DSL_GROUP_BY;
import static info.archinnov.achilles.internals.parser.TypeUtils.DSL_PACKAGE;
import static info.archinnov.achilles.internals.parser.TypeUtils.QUERY_BUILDER;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_6.SelectWhereDSLCodeGen3_6;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;

public class SelectWhereDSLCodeGen3_10 extends SelectWhereDSLCodeGen3_6 {

    @Override
    public TypeSpec.Builder buildSelectWhereForClusteringColumn(EntityMetaCodeGen.EntityMetaSignature signature,
                                                                Optional<ClassSignatureInfo> firstClusteringClassSignature,
                                                                List<ClassSignatureInfo> classesSignature,
                                                                ClassSignatureInfo lastSignature) {
        final TypeSpec.Builder builder = super.buildSelectWhereForClusteringColumn(signature, firstClusteringClassSignature, classesSignature, lastSignature);

        final String rootClassName = signature.selectClassName();
        addGroupBy(builder, rootClassName, classesSignature, lastSignature);
        return builder;
    }

    private void addGroupBy(TypeSpec.Builder clusteringClassBuilder, String rootClassName, List<ClassSignatureInfo> originalClassesSignature, ClassSignatureInfo lastSignature) {

        final LinkedList<ClassSignatureInfo> classesSignature = new LinkedList<>(originalClassesSignature);

        /**
         * Remove last ClassSignatureInfo because it does not contain any FieldSignatureInfo
         * because it represents the ClassSignatureInfo for the last query DSL class
         * and does not correspond to any partition/clustering column
         **/
        classesSignature.removeLast();


        final ClassSignatureInfo classSignature = classesSignature.get(0);

        final TypeName groupByClassTypeName = ClassName.get(DSL_PACKAGE, rootClassName
                + "." + classSignature.className
                + "." + DSL_GROUP_BY);

        TypeSpec.Builder groupByClassBuilder = TypeSpec.classBuilder(DSL_GROUP_BY)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        for (int i=0; i<classesSignature.size(); i++) {

            final List<ClassSignatureInfo> subList = classesSignature.subList(0, classesSignature.size()-i);

            final String methodName = subList
                    .stream()
                    .map(x -> x.fieldSignatureInfo.fieldName)
                    .collect(Collectors.joining("_"));

            final String groupByColumnsForJavaDoc = subList
                    .stream()
                    .map(x -> x.fieldSignatureInfo.quotedCqlColumn)
                    .collect(Collectors.joining(", "));

            final String groupByColumns = subList
                    .stream()
                    .map(x -> x.fieldSignatureInfo.quotedCqlColumn)
                    .map(quotedColumn -> "QueryBuilder.column(\"" + quotedColumn.replaceAll("\"", "\\\\\"") + "\")")
                    .collect(Collectors.joining(", "));

            groupByClassBuilder.addMethod(MethodSpec.methodBuilder(methodName)
                    .addJavadoc("SELECT ... FROM ... WHERE ... GROUP BY $L", groupByColumnsForJavaDoc)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addStatement("where.groupBy($L)", groupByColumns)
                    .addStatement("return new $T(where, cassandraOptions)", lastSignature.returnClassType)
                    .returns(lastSignature.returnClassType)
                    .build());
        }


        clusteringClassBuilder
                .addMethod(MethodSpec.methodBuilder("groupBy")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T()", groupByClassTypeName)
                        .returns(groupByClassTypeName)
                        .build())
                .addType(groupByClassBuilder.build());


    }
}
