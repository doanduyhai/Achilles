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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;

public class UpdateDSLCodeGen2_2 extends UpdateDSLCodeGen {

    @Override
    protected void augmentUpdateRelationClass(TypeSpec.Builder relationClassBuilder, FieldMetaSignature parsingResult,
                                              TypeName newTypeName, ReturnType returnType) {

        final String fieldName = parsingResult.context.fieldName;
        final String cqlColumn = parsingResult.context.quotedCqlColumn;

        final MethodSpec.Builder setFromJSONMethodBuilder = MethodSpec.methodBuilder("Set_FromJSON")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", cqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(STRING, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add($N)", fieldName)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            setFromJSONMethodBuilder.addStatement("return new $T(where)", newTypeName);
        } else {
            setFromJSONMethodBuilder.addStatement("return $T.this", newTypeName);
        }

        relationClassBuilder.addMethod(setFromJSONMethodBuilder.build());
    }
//
//    @Override
//    public void buildMethodForSimpleUpdate(TypeSpec.Builder parentBuilder, String updateColumnsClassName, TypeName newTypeName,
//                                           FieldMetaSignature parsingResult, ReturnType returnType) {
//        final String fieldName = parsingResult.context.fieldName;
//        final String cqlColumn = parsingResult.context.quotedCqlColumn;
//        final TypeName sourceType = parsingResult.sourceType;
//
//
//        final MethodSpec.Builder setMethodBuilder = MethodSpec.methodBuilder("Set")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", cqlColumn)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//
//        final MethodSpec.Builder setFromJSONMethodBuilder = MethodSpec.methodBuilder("Set_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", cqlColumn)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add($N)", fieldName)
//                .returns(newTypeName);
//
//        if (returnType == ReturnType.NEW) {
//            setMethodBuilder.addStatement("return new $T(where)", newTypeName);
//            setFromJSONMethodBuilder.addStatement("return new $T(where)", newTypeName);
//        } else {
//            setMethodBuilder.addStatement("return $T.this", newTypeName);
//            setFromJSONMethodBuilder.addStatement("return $T.this", newTypeName);
//        }
//
//        createRelationClassForColumn(parentBuilder, updateColumnsClassName, parsingResult, fieldName,
//                Arrays.asList(setMethodBuilder.build(), setFromJSONMethodBuilder.build()));
//    }
//
//    @Override
//    public void buildMethodsForListUpdate(AptUtils aptUtils, TypeSpec.Builder parentBuilder, String parentClassName,
//                                          TypeName newTypeName, FieldMetaSignature parsingResult,
//                                          ReturnType returnType) {
//        final String fieldName = parsingResult.context.fieldName;
//        final String param = fieldName + "_element";
//        final String cqlColumn = parsingResult.context.quotedCqlColumn;
//        final TypeName sourceType = parsingResult.sourceType;
//        final TypeName nestedType = aptUtils.extractTypeArgument(sourceType, 0);
//
//        List<MethodSpec> updateMethods = new ArrayList<>();
//        final MethodSpec.Builder appendTo = MethodSpec.methodBuilder("AppendTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + [?]</strong>", cqlColumn, cqlColumn)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder appendAllTo = MethodSpec.methodBuilder("AppendAllTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder prependTo = MethodSpec.methodBuilder("PrependTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = [?] + $L</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder prependAllTo = MethodSpec.methodBuilder("PrependAllTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ? + $L</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder setAtIndex = MethodSpec.methodBuilder("SetAtIndex")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = ?</strong>", fieldName)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(TypeName.INT, "index", Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", param)
//                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N))", fieldName, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeAtIndex = MethodSpec.methodBuilder("RemoveAtIndex")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = null</strong>", fieldName)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(TypeName.INT, "index", Modifier.FINAL)
//                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add(null)")
//                .addStatement("encodedValues.add(null)")
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder("RemoveFrom")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - [?]</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder("RemoveAllFrom")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder setFromJSON = MethodSpec.methodBuilder("Set_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add($N)", fieldName)
//                .returns(newTypeName);
//
//        if (returnType == ReturnType.NEW) {
//            appendTo.addStatement("return new $T(where)", newTypeName);
//            appendAllTo.addStatement("return new $T(where)", newTypeName);
//            prependTo.addStatement("return new $T(where)", newTypeName);
//            prependAllTo.addStatement("return new $T(where)", newTypeName);
//            setAtIndex.addStatement("return new $T(where)", newTypeName);
//            removeAtIndex.addStatement("return new $T(where)", newTypeName);
//            removeFrom.addStatement("return new $T(where)", newTypeName);
//            removeAllFrom.addStatement("return new $T(where)", newTypeName);
//            set.addStatement("return new $T(where)", newTypeName);
//            setFromJSON.addStatement("return new $T(where)", newTypeName);
//        } else {
//            appendTo.addStatement("return $T.this", newTypeName);
//            appendAllTo.addStatement("return $T.this", newTypeName);
//            prependTo.addStatement("return $T.this", newTypeName);
//            prependAllTo.addStatement("return $T.this", newTypeName);
//            setAtIndex.addStatement("return $T.this", newTypeName);
//            removeAtIndex.addStatement("return $T.this", newTypeName);
//            removeFrom.addStatement("return $T.this", newTypeName);
//            removeAllFrom.addStatement("return $T.this", newTypeName);
//            set.addStatement("return $T.this", newTypeName);
//            setFromJSON.addStatement("return $T.this", newTypeName);
//        }
//
//        updateMethods.add(appendTo.build());
//        updateMethods.add(appendAllTo.build());
//        updateMethods.add(prependTo.build());
//        updateMethods.add(prependAllTo.build());
//        updateMethods.add(setAtIndex.build());
//        updateMethods.add(removeAtIndex.build());
//        updateMethods.add(removeFrom.build());
//        updateMethods.add(removeAllFrom.build());
//        updateMethods.add(set.build());
//        updateMethods.add(setFromJSON.build());
//
//        createRelationClassForColumn(parentBuilder, parentClassName, parsingResult, fieldName, updateMethods);
//    }
//
//    @Override
//    public void buildMethodsForSetUpdate(AptUtils aptUtils, TypeSpec.Builder parentBuilder, String parentClassName,
//                                         TypeName newTypeName, FieldMetaSignature parsingResult,
//                                         ReturnType returnType) {
//        final String fieldName = parsingResult.context.fieldName;
//        final String param = fieldName + "_element";
//        final String cqlColumn = parsingResult.context.quotedCqlColumn;
//        final TypeName sourceType = parsingResult.sourceType;
//        final TypeName nestedType = aptUtils.extractTypeArgument(sourceType, 0);
//
//        List<MethodSpec> updateMethods = new ArrayList<>();
//        final MethodSpec.Builder addTo = MethodSpec.methodBuilder("AddTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + {?}</strong>", cqlColumn, cqlColumn)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N)))", fieldName, SETS, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder addToFromJSON = MethodSpec.methodBuilder("AddTo_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + {fromJson(?)}</strong>", cqlColumn, cqlColumn)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, param, Modifier.FINAL)
//                .addStatement("where.with($T.add($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", param)
//                .addStatement("encodedValues.add($N)", param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder("AddAllTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder("RemoveFrom")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - {?}</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedType, param, Modifier.FINAL)
//                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N)))", fieldName, SETS, param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeFromFromJSON = MethodSpec.methodBuilder("RemoveFrom_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - {fromJson(?)}</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, param, Modifier.FINAL)
//                .addStatement("where.with($T.remove($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", param)
//                .addStatement("encodedValues.add($N)", param)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder("RemoveAllFrom")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder setFromJSON = MethodSpec.methodBuilder("Set_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add($N)", fieldName)
//                .returns(newTypeName);
//
//        if (returnType == ReturnType.NEW) {
//            addTo.addStatement("return new $T(where)", newTypeName);
//            addToFromJSON.addStatement("return new $T(where)", newTypeName);
//            addAllTo.addStatement("return new $T(where)", newTypeName);
//            removeFrom.addStatement("return new $T(where)", newTypeName);
//            removeFromFromJSON.addStatement("return new $T(where)", newTypeName);
//            removeAllFrom.addStatement("return new $T(where)", newTypeName);
//            set.addStatement("return new $T(where)", newTypeName);
//            setFromJSON.addStatement("return new $T(where)", newTypeName);
//        } else {
//            addTo.addStatement("return $T.this", newTypeName);
//            addToFromJSON.addStatement("return $T.this", newTypeName);
//            addAllTo.addStatement("return $T.this", newTypeName);
//            removeFrom.addStatement("return $T.this", newTypeName);
//            removeFromFromJSON.addStatement("return $T.this", newTypeName);
//            removeAllFrom.addStatement("return $T.this", newTypeName);
//            set.addStatement("return $T.this", newTypeName);
//            setFromJSON.addStatement("return $T.this", newTypeName);
//        }
//
//        updateMethods.add(addTo.build());
//        updateMethods.add(addToFromJSON.build());
//        updateMethods.add(addAllTo.build());
//        updateMethods.add(removeFrom.build());
//        updateMethods.add(removeFromFromJSON.build());
//        updateMethods.add(removeAllFrom.build());
//        updateMethods.add(set.build());
//        updateMethods.add(setFromJSON.build());
//
//        createRelationClassForColumn(parentBuilder, parentClassName, parsingResult, fieldName, updateMethods);
//    }
//
//    @Override
//    public void buildMethodsForMapUpdate(AptUtils aptUtils, TypeSpec.Builder parentBuilder, String parentClassName,
//                                         TypeName newTypeName, FieldMetaSignature parsingResult,
//                                         ReturnType returnType) {
//        final String fieldName = parsingResult.context.fieldName;
//        final String paramKey = fieldName + "_key";
//        final String paramValue = fieldName + "_value";
//        final String cqlColumn = parsingResult.context.quotedCqlColumn;
//        final TypeName sourceType = parsingResult.sourceType;
//        final TypeName nestedKeyType = aptUtils.extractTypeArgument(sourceType, 0);
//        final TypeName nestedValueType = aptUtils.extractTypeArgument(sourceType, 1);
//
//        List<MethodSpec> updateMethods = new ArrayList<>();
//        final MethodSpec.Builder putTo = MethodSpec.methodBuilder("PutTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = ?</strong>", cqlColumn)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
//                .addParameter(nestedValueType, paramValue, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add($N)", paramValue)
//                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N))", fieldName, paramKey)
//                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N))", fieldName, paramValue)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder putToKeyFromJSON = MethodSpec.methodBuilder("PutTo_KeyFromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[fromJson(?)] = ?</strong>", cqlColumn)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, paramKey, Modifier.FINAL)
//                .addParameter(nestedValueType, paramValue, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.fromJson($T.bindMarker($S)), $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add($N)", paramValue)
//                .addStatement("encodedValues.add($N)", paramKey)
//                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N))", fieldName, paramValue)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder putToValueFromJSON = MethodSpec.methodBuilder("PutTo_ValueFromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = fromJson(?)</strong>", cqlColumn)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
//                .addParameter(STRING, paramValue, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add($N)", paramValue)
//                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N))", fieldName, paramKey)
//                .addStatement("encodedValues.add($N)", paramValue)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder putToKeyValueFromJSON = MethodSpec.methodBuilder("PutTo_KeyValueFromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[fromJson(?)] = fromJson(?)</strong>", cqlColumn)
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, paramKey, Modifier.FINAL)
//                .addParameter(STRING, paramValue, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.fromJson($T.bindMarker($S)), $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, paramKey, QUERY_BUILDER, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add($N)", paramValue)
//                .addStatement("encodedValues.add($N)", paramKey)
//                .addStatement("encodedValues.add($N)", paramValue)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder("AddAllTo")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeByKey = MethodSpec.methodBuilder("RemoveByKey")
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = null</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add(null)")
//                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N))", fieldName, paramKey)
//                .addStatement("encodedValues.add(null)")
//                .returns(newTypeName);
//
//        final MethodSpec.Builder removeByKeyFromJSON = MethodSpec.methodBuilder("RemoveByKey_FromJSON")
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[fromJson(?)] = null</strong>", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addParameter(STRING, paramKey, Modifier.FINAL)
//                .addStatement("where.with($T.put($S, $T.fromJson($T.bindMarker($S)), $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
//                .addStatement("boundValues.add($N)", paramKey)
//                .addStatement("boundValues.add(null)")
//                .addStatement("encodedValues.add($N)", paramKey)
//                .addStatement("encodedValues.add(null)")
//                .returns(newTypeName);
//
//        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(sourceType, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
//                .returns(newTypeName);
//
//        final MethodSpec.Builder setFromJSON = MethodSpec.methodBuilder("Set_FromJSON")
//                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = fromJson(?)", fieldName)
//                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
//                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
//                .addParameter(STRING, fieldName, Modifier.FINAL)
//                .addStatement("where.with($T.set($S, $T.fromJson($T.bindMarker($S))))",
//                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, QUERY_BUILDER, cqlColumn)
//                .addStatement("boundValues.add($N)", fieldName)
//                .addStatement("encodedValues.add($N)", fieldName)
//                .returns(newTypeName);
//
//        if (returnType == ReturnType.NEW) {
//            putTo.addStatement("return new $T(where)", newTypeName);
//            putToKeyFromJSON.addStatement("return new $T(where)", newTypeName);
//            putToValueFromJSON.addStatement("return new $T(where)", newTypeName);
//            putToKeyValueFromJSON.addStatement("return new $T(where)", newTypeName);
//            addAllTo.addStatement("return new $T(where)", newTypeName);
//            removeByKey.addStatement("return new $T(where)", newTypeName);
//            removeByKeyFromJSON.addStatement("return new $T(where)", newTypeName);
//            set.addStatement("return new $T(where)", newTypeName);
//            setFromJSON.addStatement("return new $T(where)", newTypeName);
//        } else {
//            putTo.addStatement("return $T.this", newTypeName);
//            putToKeyFromJSON.addStatement("return $T.this", newTypeName);
//            putToValueFromJSON.addStatement("return $T.this", newTypeName);
//            putToKeyValueFromJSON.addStatement("return $T.this", newTypeName);
//            addAllTo.addStatement("return $T.this", newTypeName);
//            removeByKey.addStatement("return $T.this", newTypeName);
//            removeByKeyFromJSON.addStatement("return $T.this", newTypeName);
//            set.addStatement("return $T.this", newTypeName);
//            setFromJSON.addStatement("return $T.this", newTypeName);
//        }
//
//        updateMethods.add(putTo.build());
//        updateMethods.add(putToKeyFromJSON.build());
//        updateMethods.add(putToValueFromJSON.build());
//        updateMethods.add(putToKeyValueFromJSON.build());
//        updateMethods.add(addAllTo.build());
//        updateMethods.add(removeByKey.build());
//        updateMethods.add(removeByKeyFromJSON.build());
//        updateMethods.add(set.build());
//        updateMethods.add(setFromJSON.build());
//
//        createRelationClassForColumn(parentBuilder, parentClassName, parsingResult, fieldName, updateMethods);
//    }
}
