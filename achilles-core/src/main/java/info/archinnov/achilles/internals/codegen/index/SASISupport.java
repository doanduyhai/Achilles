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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;

public interface SASISupport {

    default MethodSpec buildSASIStartWith(TypeName nextType, AbstractDSLCodeGen.FieldSignatureInfo fieldInfo, AbstractDSLCodeGen.ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("StartWith")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L LIKE '?%' </strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.like($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N + $S)", fieldInfo.fieldName, "%")
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N + $S, $T.of(cassandraOptions)))", fieldInfo.fieldName, fieldInfo.fieldName, "%", OPTIONAL)
                .returns(nextType);

        if (returnType == AbstractDSLCodeGen.ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildSASIEndWith(TypeName nextType, AbstractDSLCodeGen.FieldSignatureInfo fieldInfo, AbstractDSLCodeGen.ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("EndWith")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L LIKE '%?' </strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.like($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($S + $N)", "%", fieldInfo.fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($S + $N, $T.of(cassandraOptions)))", fieldInfo.fieldName, "%", fieldInfo.fieldName, OPTIONAL)
                .returns(nextType);

        if (returnType == AbstractDSLCodeGen.ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildSASIContains(TypeName nextType, AbstractDSLCodeGen.FieldSignatureInfo fieldInfo, AbstractDSLCodeGen.ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("Contains")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L LIKE '%?%' </strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.like($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($S + $N + $S)", "%", fieldInfo.fieldName, "%")
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($S + $N + $S, $T.of(cassandraOptions)))", fieldInfo.fieldName, "%", fieldInfo.fieldName, "%", OPTIONAL)
                .returns(nextType);

        if (returnType == AbstractDSLCodeGen.ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }

    default MethodSpec buildSASILike(TypeName nextType, AbstractDSLCodeGen.FieldSignatureInfo fieldInfo, AbstractDSLCodeGen.ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("Like")
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L LIKE '?' </strong>", fieldInfo.quotedCqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldInfo.fieldName)
                .addStatement("where.and($T.like($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, fieldInfo.quotedCqlColumn, QUERY_BUILDER, fieldInfo.quotedCqlColumn)
                .addStatement("boundValues.add($N)", fieldInfo.fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldInfo.fieldName, fieldInfo.fieldName, OPTIONAL)
                .returns(nextType);

        if (returnType == AbstractDSLCodeGen.ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", nextType);
        } else {
            builder.addStatement("return $T.this", nextType);
        }
        return builder.build();
    }
}
