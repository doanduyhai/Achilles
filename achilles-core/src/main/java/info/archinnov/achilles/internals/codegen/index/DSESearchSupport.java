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

package info.archinnov.achilles.internals.codegen.index;

import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.*;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.DSESearchInfoContext;

public interface DSESearchSupport {

    default void buildDSESearchIndexRelation(EntityMetaCodeGen.EntityMetaSignature signature,
                                                         TypeSpec.Builder indexSelectWhereBuilder,
                                                         AugmentRelationClassForWhereClauseLambda augmentRelationClassForWhereClauseLambda,
                                                         String parentClassName,
                                                         ClassSignatureInfo lastSignature,
                                                         ReturnType returnType) {

        final List<IndexFieldSignatureInfo> dseSearchColumns = getDSESearchColsSignatureInfo(signature.fieldMetaSignatures);

        dseSearchColumns.forEach(fieldInfo -> {

            final String relationClassName = "Indexed_" + upperCaseFirst(fieldInfo.fieldName);
            TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, parentClassName + "." + relationClassName);

            final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(relationClassName)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

            final TypeName targetType = fieldInfo.typeName;

            final DSESearchInfoContext dseSearchInfoContext = fieldInfo.indexInfo.dseSearchInfoContext.get();

            if (targetType.equals(STRING)) {
                if (dseSearchInfoContext.fullTextSearchEnabled) {
                    relationClassBuilder.addMethod(buildDSETextStartWith(lastSignature.returnClassType, fieldInfo, returnType));
                    relationClassBuilder.addMethod(buildDSETextEndWith(lastSignature.returnClassType, fieldInfo, returnType));
                    relationClassBuilder.addMethod(buildDSETextContains(lastSignature.returnClassType, fieldInfo, returnType));
                }
                relationClassBuilder.addMethod(buildDSESingleRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
            } else if (targetType.equals(JAVA_UTIL_DATE)) {
                relationClassBuilder.addMethod(buildDSESingleDateRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleDateRelation(GT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleDateRelation(GTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleDateRelation(LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleDateRelation(LTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GT, LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GT, LTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GTE, LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleDateRelation(GTE, LTE, lastSignature.returnClassType, fieldInfo, returnType));
            } else {
                relationClassBuilder.addMethod(buildDSESingleRelation(EQ, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleRelation(GT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleRelation(GTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleRelation(LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSESingleRelation(LTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleRelation(GT, LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleRelation(GT, LTE, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleRelation(GTE, LT, lastSignature.returnClassType, fieldInfo, returnType));
                relationClassBuilder.addMethod(buildDSEDoubleRelation(GTE, LTE, lastSignature.returnClassType, fieldInfo, returnType));
            }

            relationClassBuilder.addMethod(buildDSERawPredicate(lastSignature.returnClassType, fieldInfo, returnType));

            augmentRelationClassForWhereClauseLambda.augmentRelationClassForWhereClause(relationClassBuilder, fieldInfo, lastSignature, returnType);

            indexSelectWhereBuilder.addType(relationClassBuilder.build());

            indexSelectWhereBuilder.addMethod(DSESearchSupport.buildSearchRelationMethod(fieldInfo.fieldName, relationClassTypeName));
        });

        //Do not generate rawSolrQuery() method in SelectEnd
        if (returnType == ReturnType.NEW) {
            indexSelectWhereBuilder.addMethod(buildDSERawSolrQuery(lastSignature.returnClassType));
        }
    }

    default MethodSpec buildDSETextStartWith(TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("StartWith")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:?*'</strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($S + $N + $S)", fieldInfo.quotedCqlColumn + ":",
                        fieldInfo.fieldName, "*")
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($S + $N)", fieldInfo.quotedCqlColumn + ":*", fieldInfo.fieldName)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($S + $N + $S)", fieldInfo.quotedCqlColumn + ":*", fieldInfo.fieldName, "*")
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($T.format($S, $S, meta.$L.encodeFromJava($N, $T.of(cassandraOptions))))",
                        STRING, relationToSolrSyntaxForQuery(relation),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName,
                        param,
                        OPTIONAL)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($T.format($S, $S, dateFormat.format(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))))",
                        STRING, queryString,
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param,
                        OPTIONAL)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($T.format($S, $S, meta.$L.encodeFromJava($N, $T.of(cassandraOptions)), meta.$L.encodeFromJava($N, $T.of(cassandraOptions))))",
                        STRING, relationToSolrSyntaxForQuery(relation1, relation2),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param1, OPTIONAL,
                        fieldInfo.fieldName, param2, OPTIONAL)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($T.format($S, $S, dateFormat.format(meta.$L.encodeFromJava($N, $T.of(cassandraOptions))), dateFormat.format(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))))",
                        STRING, relationToSolrSyntaxForQuery(relation1, relation2),
                        fieldInfo.quotedCqlColumn,
                        fieldInfo.fieldName, param1, OPTIONAL,
                        fieldInfo.fieldName, param2, OPTIONAL)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSERawPredicate(TypeName nextType, FieldSignatureInfo fieldInfo, ReturnType returnType) {

        final String param = "rawSolrPredicate";
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("RawPredicate")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='$L:?'</strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param)
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.appendToSolrQuery($T.format($S, $S, $N))",
                        STRING, "%s:%s", fieldInfo.quotedCqlColumn, param)
                .returns(nextType);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildDSERawSolrQuery(TypeName nextType) {

        final String param = "solr_query";
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("rawSolrQuery")
                .addJavadoc("Inject a raw Solr query string for DSE Search. Example <em>...where().RawSolrQuery(\"(firstname:John* OR lastname:John*) AND age:[30 TO 40]\").getList();</em>")
                .addJavadoc("<br/>")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>solr_query='?'</strong>")
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, param)
                .beginControlFlow("if(!cassandraOptions.hasSolrQuery())")
                .addStatement("where.and($T.eq($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, "solr_query", QUERY_BUILDER, "solr_query")
                .endControlFlow()
                .addStatement("cassandraOptions.rawSolrQuery($N)", param)
                .returns(nextType);

        builder.addStatement("return new $T(where, cassandraOptions)", nextType);
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

    @FunctionalInterface
    interface AugmentRelationClassForWhereClauseLambda {
        void augmentRelationClassForWhereClause (TypeSpec.Builder relationClassBuilder,
                                                        FieldSignatureInfo fieldInfo,
                                                        ClassSignatureInfo lastSignature,
                                                        ReturnType returnType);
    }

    static MethodSpec buildSearchRelationMethod(String fieldName, TypeName relationClassTypeName) {
        return MethodSpec.methodBuilder("search_on_" + fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", relationClassTypeName)
                .returns(relationClassTypeName)
                .build();
    }
}
