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

package info.archinnov.achilles.internals.codegen.index;

import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import java.text.SimpleDateFormat;
import java.util.Date;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.FieldSignatureInfo;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType;

public interface DSESearchSupport {

    default MethodSpec buildDSETextStartWith(TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("StartWith")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:?*'</strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($S + $N + $S)", fieldInfo.quotedCqlColumn + ":",
                        fieldInfo.fieldName, "*")
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSETextEndWith(TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("EndWith")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:*?'</strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($S + $N)", fieldInfo.quotedCqlColumn + ":*", fieldInfo.fieldName)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSETextContains(TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("Contains")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:*?*'</strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($S + $N + $S)", fieldInfo.quotedCqlColumn + ":*", fieldInfo.fieldName, "*")
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSESingleRelation(String relation, TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {

        final String param = fieldInfo.fieldName;
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(upperCaseFirst(relation))
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:$S'</strong>", fieldInfo.quotedCqlColumn, relationToSolrSyntaxForJavadoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($T.format($S, $S, meta.$L.encodeFromJava($N)))",
                        STRING, relationToSolrSyntaxForQuery(relation),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSESingleDateRelation(String relation, TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final String param = fieldInfo.fieldName;

        final String queryString = relation.equals(EQ)
                ? "%s:\"%s\""
                : relationToSolrSyntaxForQuery(relation);

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(upperCaseFirst(relation))
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:$S'</strong>", fieldInfo.quotedCqlColumn, relationToSolrSyntaxForJavadoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param)
                .addStatement("$T dateFormat = new $T($T.SOLR_DATE_FORMAT)", SIMPLE_DATE_FORMAT, SIMPLE_DATE_FORMAT, DSE_SEARCH_ANNOT)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($T.format($S, $S, dateFormat.format(meta.$L.encodeFromJava($N))))",
                        STRING, queryString,
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSEDoubleRelation(String relation1, String relation2, TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final String param1 = fieldInfo.fieldName + "_" + relation1;
        final String param2 = fieldInfo.fieldName + "_" + relation2;
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2))
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:$S'</strong>", fieldInfo.quotedCqlColumn, relationToSolrSyntaxForJavadoc(relation1, relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param1)
                .addParameter(fieldInfo.typeName, param2)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($T.format($S, $S, meta.$L.encodeFromJava($N), meta.$L.encodeFromJava($N)))",
                        STRING, relationToSolrSyntaxForQuery(relation1, relation2),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param1, fieldInfo.fieldName, param2)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSEDoubleDateRelation(String relation1, String relation2, TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final String param1 = fieldInfo.fieldName + "_" + relation1;
        final String param2 = fieldInfo.fieldName + "_" + relation2;
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(upperCaseFirst(relation1) + "_And_" + upperCaseFirst(relation2))
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:$S'</strong>", fieldInfo.quotedCqlColumn, relationToSolrSyntaxForJavadoc(relation1, relation2))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, param1)
                .addParameter(fieldInfo.typeName, param2)
                .addStatement("$T dateFormat = new $T($T.SOLR_DATE_FORMAT)", SIMPLE_DATE_FORMAT, SIMPLE_DATE_FORMAT, DSE_SEARCH_ANNOT)
                .beginControlFlow("if(!options.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("options.appendToSolrQuery($T.format($S, $S, dateFormat.format(meta.$L.encodeFromJava($N)), dateFormat.format(meta.$L.encodeFromJava($N))))",
                        STRING, relationToSolrSyntaxForQuery(relation1, relation2),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param1, fieldInfo.fieldName, param2)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default String relationToSolrSyntaxForJavadoc(String relation) {
        switch (relation) {
            case EQ:
                return "?";
            case LT:
                return "[* TO ?}";
            case LTE:
                return "[* TO ?]";
            case GT:
                return "{? TO *]";
            case GTE:
                return "[? TO *]";
            default:
                return " ??? ";
        }
    }

    default String relationToSolrSyntaxForJavadoc(String relation1, String relation2) {
        if (relation1.equals(GT) && relation2.equals(LT)) {
            return "{? TO ?}";
        } else if (relation1.equals(GT) && relation2.equals(LTE)) {
            return "{? TO ?]";
        } else if (relation1.equals(GTE) && relation2.equals(LT)) {
            return "[? TO ?}";
        } else if (relation1.equals(GTE) && relation2.equals(LTE)) {
            return "[? TO ?]";
        } else {
            return "???";
        }
    }

    default String relationToSolrSyntaxForQuery(String relation) {
        switch (relation) {
            case EQ:
                return "%s:%s";
            case LT:
                return "%s:[* TO %s}";
            case LTE:
                return "%s:[* TO %s]";
            case GT:
                return "%s:{%s TO *]";
            case GTE:
                return "%s:[%s TO *]";
            default:
                return " ??? ";
        }
    }

    default String relationToSolrSyntaxForQuery(String relation1, String relation2) {
        if (relation1.equals(GT) && relation2.equals(LT)) {
            return "%s:{%s TO %s}";
        } else if (relation1.equals(GT) && relation2.equals(LTE)) {
            return "%s:{%s TO %s]";
        } else if (relation1.equals(GTE) && relation2.equals(LT)) {
            return "%s:[%s TO %s}";
        } else if (relation1.equals(GTE) && relation2.equals(LTE)) {
            return "%s:[%s TO %s]";
        } else {
            return "???";
        }
    }
}
