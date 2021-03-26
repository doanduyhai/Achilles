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

package info.archinnov.achilles.internals.codegen.dsl.update;

import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.*;
import javax.lang.model.element.Modifier;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.type.tuples.Tuple2;

public abstract class UpdateDSLCodeGen extends AbstractDSLCodeGen {

    public static final Comparator<Tuple2<String, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            (o1, o2) -> o1._2().order.compareTo(o2._2().order);

    protected abstract void augmentUpdateRelationClass(ParentSignature parentSignature,
                                                       FieldMetaSignature parsingResult,
                                                       TypeName newTypeName,
                                                       ReturnType returnType);

    public TypeSpec buildUpdateClass(AptUtils aptUtils, EntityMetaSignature signature, UpdateWhereDSLCodeGen updateWhereDSLCodeGen) {

        final String firstPartitionKey = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        String updateClassName = signature.updateClassName();
        TypeName updateFromTypeName = ClassName.get(DSL_PACKAGE, signature.updateFromReturnType());

        TypeName updateColumnsTypeName = ClassName.get(DSL_PACKAGE, signature.updateColumnsReturnType());

        TypeName updateWhereTypeName = ClassName.get(DSL_PACKAGE, signature.updateWhereReturnType(firstPartitionKey));

        final List<ColumnType> candidateColumns = Arrays.asList(NORMAL, STATIC, COUNTER, STATIC_COUNTER);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateClassName)
                .superclass(ABSTRACT_UPDATE)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildUpdateConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addMethod(buildFromBaseTableMethod(updateFromTypeName))
                .addMethod(buildFromSchemaProviderMethod(updateFromTypeName))
                .addType(buildUpdateColumns(aptUtils, signature, COLUMNS_DSL_SUFFIX,
                        updateColumnsTypeName, updateWhereTypeName, candidateColumns))
                .addType(buildUpdateFrom(aptUtils, signature, FROM_DSL_SUFFIX, updateColumnsTypeName, candidateColumns));


        updateWhereDSLCodeGen.buildWhereClasses(signature).forEach(builder::addType);

        return builder.build();
    }

    public TypeSpec buildUpdateStaticClass(AptUtils aptUtils, EntityMetaSignature signature, UpdateWhereDSLCodeGen updateWhereDSLCodeGen) {

        final String firstPartitionKey = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        String updateStaticClassName = signature.updateStaticClassName();
        TypeName updateStaticFromTypeName = ClassName.get(DSL_PACKAGE, signature.updateStaticFromReturnType());

        TypeName updateStaticColumnsTypeName = ClassName.get(DSL_PACKAGE, signature.updateStaticColumnsReturnType());

        TypeName updateStaticWhereTypeName = ClassName.get(DSL_PACKAGE, signature.updateStaticWhereReturnType(firstPartitionKey));

        final List<ColumnType> candidateColumns = Arrays.asList(STATIC, STATIC_COUNTER);
        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateStaticClassName)
                .superclass(ABSTRACT_UPDATE)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildUpdateConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addMethod(buildFromBaseTableMethod(updateStaticFromTypeName))
                .addMethod(buildFromSchemaProviderMethod(updateStaticFromTypeName))
                .addType(buildUpdateColumns(aptUtils, signature, COLUMNS_DSL_SUFFIX,
                        updateStaticColumnsTypeName, updateStaticWhereTypeName, candidateColumns))
                .addType(buildUpdateFrom(aptUtils, signature, FROM_DSL_SUFFIX, updateStaticColumnsTypeName, candidateColumns));


        updateWhereDSLCodeGen.buildWhereClassesForStatic(signature).forEach(builder::addType);

        return builder.build();
    }

    public MethodSpec buildUpdateConstructor(EntityMetaSignature signature) {
        String metaClassName = signature.className + META_SUFFIX;
        TypeName metaClassType = ClassName.get(ENTITY_META_PACKAGE, metaClassName);

        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(RUNTIME_ENGINE, "rte")
                .addParameter(metaClassType, "meta")
                .addStatement("super(rte)")
                .addStatement("this.meta = meta")
                .build();

    }

    public MethodSpec buildFromSchemaProviderMethod(TypeName updateFromTypeName) {
        return MethodSpec.methodBuilder("from")
                .addJavadoc("Generate an UPDATE <strong>FROM</strong> ... using the given SchemaNameProvider")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $T.update(currentKeyspace, currentTable).where()", UPDATE_DOT_WHERE, QUERY_BUILDER)
                .addStatement("return new $T(where, $T.withSchemaNameProvider(schemaNameProvider))", updateFromTypeName, OPTIONS)
                .returns(updateFromTypeName)
                .build();
    }

    public MethodSpec buildFromBaseTableMethod(TypeName updateFromTypeName) {
        return MethodSpec.methodBuilder("fromBaseTable")
                .addJavadoc("Generate an UPDATE <strong>FROM</strong> ...")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final String currentKeyspace = meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName())",
                        "unknown_keyspace_for_")
                .addStatement("final $T where = $T.update(currentKeyspace, meta.getTableOrViewName()).where()", UPDATE_DOT_WHERE, QUERY_BUILDER)
                .addStatement("return new $T(where, new $T())", updateFromTypeName, OPTIONS)
                .returns(updateFromTypeName)
                .build();
    }

    public TypeSpec buildUpdateFrom(AptUtils aptUtils, EntityMetaSignature signature,
                                            String updateFromClassName,
                                            TypeName updateColumnsTypeName,
                                            List<ColumnType> candidateColumns) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateFromClassName)
                .superclass(ABSTRACT_UPDATE_FROM)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(UPDATE_DOT_WHERE, "where")
                        .addParameter(OPTIONS, "cassandraOptions")
                        .addStatement("super(where, cassandraOptions)")
                        .build());

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> candidateColumns.contains(x.context.columnType))
                .forEach(x -> buildUpdateColumnMethods(ParentSignature.of(aptUtils, builder, updateFromClassName, Optional.empty(), Optional.empty()),
                        updateColumnsTypeName, x, ReturnType.NEW));

        return builder.build();
    }

    public TypeSpec buildUpdateColumns(AptUtils aptUtils, EntityMetaSignature signature,
                                               String updateColumnsClassName,
                                               TypeName updateColumnsTypeName,
                                               TypeName updateWhereTypeName,
                                               List<ColumnType> candidateColumns) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateColumnsClassName)
                .superclass(ABSTRACT_UPDATE_COLUMNS)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(UPDATE_DOT_WHERE, "where")
                        .addParameter(OPTIONS, "cassandraOptions")
                        .addStatement("super(where, cassandraOptions)")
                        .build());

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> candidateColumns.contains(x.context.columnType))
                .forEach(x -> buildUpdateColumnMethods(ParentSignature.of(aptUtils, builder, updateColumnsClassName, Optional.empty(), Optional.empty()),
                        updateColumnsTypeName, x, ReturnType.THIS));

        builder.addMethod(MethodSpec.methodBuilder("where")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(where, cassandraOptions)", updateWhereTypeName)
                .returns(updateWhereTypeName)
                .build());

        return builder.build();

    }

    public void buildUpdateColumnMethods(ParentSignature parentSignature,
                                         TypeName nextTypeName,
                                         FieldMetaSignature fieldMeta,
                                         ReturnType returnType) {

        final ColumnType columnType = fieldMeta.context.columnType;
        final boolean isCounterColumn = columnType == COUNTER || columnType == STATIC_COUNTER;

        if (fieldMeta.isList()) {
            buildMethodsForListUpdate(parentSignature, nextTypeName, fieldMeta, returnType);
        } else if (fieldMeta.isSet()) {
            buildMethodsForSetUpdate(parentSignature, nextTypeName, fieldMeta, returnType);
        } else if (fieldMeta.isMap()) {
            buildMethodsForMapUpdate(parentSignature, nextTypeName, fieldMeta, returnType);
        } else if (isCounterColumn) {
            buildMethodsForCounterUpdate(parentSignature, nextTypeName, fieldMeta, returnType);
        } else {
            buildMethodForSimpleUpdate(parentSignature, nextTypeName, fieldMeta, returnType);
        }
    }

    public void buildMethodForSimpleUpdate(ParentSignature parentSignature,
                                           TypeName newTypeName,
                                           FieldMetaSignature parsingResult,
                                           ReturnType returnType) {

        final String fieldName = parentSignature.parentFieldName
                .map(x -> String.format("%s.udtClassProperty.%s",x, parsingResult.context.fieldName))
                .orElse(parsingResult.context.fieldName);

        final String param = parentSignature.parentFieldName
                .map(x -> x + "_" + parsingResult.context.fieldName)
                .orElse(parsingResult.context.fieldName);

        final String cqlColumn = parentSignature.parentQuotedCQLColumn
                .map(x -> x + "." + parsingResult.context.quotedCqlColumn)
                .orElse(parsingResult.context.quotedCqlColumn);

        final TypeName sourceType = parsingResult.sourceType;

        QueryBuilder
                .update("")
                .with()
                .and(QueryBuilder.set("",""));

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", cqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.of($S, $T.bindMarker($S)))",
                        NON_ESCAPING_ASSIGNMENT, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            builder.addStatement("return $T.this", newTypeName);
        }

        createRelationClassForColumn(parentSignature,
                parsingResult, newTypeName,
                returnType, Arrays.asList(builder.build()));
    }


    public void buildMethodsForListUpdate(ParentSignature parentSignature,
                                          TypeName newTypeName,
                                          FieldMetaSignature fieldMetaSignature,
                                          ReturnType returnType) {

        final String fieldName = parentSignature.parentFieldName
                .map(x -> String.format("%s.udtClassProperty.%s",x, fieldMetaSignature.context.fieldName))
                .orElse(fieldMetaSignature.context.fieldName);

        final String param = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMetaSignature.context.fieldName + "_element")
                .orElse(fieldMetaSignature.context.fieldName + "_element");

        final String cqlColumn = parentSignature.parentQuotedCQLColumn
                .map(x -> x + "." + fieldMetaSignature.context.quotedCqlColumn)
                .orElse(fieldMetaSignature.context.quotedCqlColumn);

        final TypeName sourceType = fieldMetaSignature.sourceType;
        final TypeName nestedType = parentSignature.aptUtils.extractTypeArgument(sourceType, 0);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder appendTo = MethodSpec.methodBuilder("AppendTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + [?]</strong>", cqlColumn, cqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N), $T.of(cassandraOptions)))", fieldName, ARRAYS, param, OPTIONAL)
                .returns(newTypeName);


        final MethodSpec.Builder appendAllTo = MethodSpec.methodBuilder("AppendAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);


        final MethodSpec.Builder prependTo = MethodSpec.methodBuilder("PrependTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = [?] + $L</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N), $T.of(cassandraOptions)))", fieldName, ARRAYS, param, OPTIONAL)
                .returns(newTypeName);


        final MethodSpec.Builder prependAllTo = MethodSpec.methodBuilder("PrependAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ? + $L</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder setAtIndex = MethodSpec.methodBuilder("SetAtIndex")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = ?</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT, "index", Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder removeAtIndex = MethodSpec.methodBuilder("RemoveAtIndex")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = null</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT, "index", Modifier.FINAL)
                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add(null)")
                .addStatement("encodedValues.add(null)")
                .returns(newTypeName);

        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder("RemoveFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - [?]</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N), $T.of(cassandraOptions)))", fieldName, ARRAYS, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder("RemoveAllFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.of($S, $T.bindMarker($S)))",
                        NON_ESCAPING_ASSIGNMENT, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            appendTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            appendAllTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            prependTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            prependAllTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            setAtIndex.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeAtIndex.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeFrom.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeAllFrom.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            set.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            appendTo.addStatement("return $T.this", newTypeName);
            appendAllTo.addStatement("return $T.this", newTypeName);
            prependTo.addStatement("return $T.this", newTypeName);
            prependAllTo.addStatement("return $T.this", newTypeName);
            setAtIndex.addStatement("return $T.this", newTypeName);
            removeAtIndex.addStatement("return $T.this", newTypeName);
            removeFrom.addStatement("return $T.this", newTypeName);
            removeAllFrom.addStatement("return $T.this", newTypeName);
            set.addStatement("return $T.this", newTypeName);
        }

        // Extra update methods only for non frozen collections
        if (!fieldMetaSignature.context.columnInfo.frozen == true) {
            updateMethods.add(appendTo.build());
            updateMethods.add(appendAllTo.build());
            updateMethods.add(prependTo.build());
            updateMethods.add(prependAllTo.build());
            updateMethods.add(setAtIndex.build());
            updateMethods.add(removeAtIndex.build());
            updateMethods.add(removeFrom.build());
            updateMethods.add(removeAllFrom.build());
        }

        updateMethods.add(set.build());

        createRelationClassForColumn(parentSignature,
                fieldMetaSignature, newTypeName,
                returnType, updateMethods);
    }

    public void buildMethodsForSetUpdate(ParentSignature parentSignature,
                                         TypeName newTypeName,
                                         FieldMetaSignature fieldMetaSignature,
                                         ReturnType returnType) {
        final String fieldName = parentSignature.parentFieldName
                .map(x -> String.format("%s.udtClassProperty.%s",x, fieldMetaSignature.context.fieldName))
                .orElse(fieldMetaSignature.context.fieldName);

        final String param = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMetaSignature.context.fieldName + "_element")
                .orElse(fieldMetaSignature.context.fieldName + "_element");

        final String cqlColumn = parentSignature.parentQuotedCQLColumn
                .map(x -> x + "." + fieldMetaSignature.context.quotedCqlColumn)
                .orElse(fieldMetaSignature.context.quotedCqlColumn);

        final TypeName sourceType = fieldMetaSignature.sourceType;
        final TypeName nestedType = parentSignature.aptUtils.extractTypeArgument(sourceType, 0);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder addTo = MethodSpec.methodBuilder("AddTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + {?}</strong>", cqlColumn, cqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N), $T.of(cassandraOptions)))", fieldName, SETS, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder("AddAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder("RemoveFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - {?}</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N), $T.of(cassandraOptions)))", fieldName, SETS, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder("RemoveAllFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.of($S, $T.bindMarker($S)))",
                        NON_ESCAPING_ASSIGNMENT, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            addTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            addAllTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeFrom.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeAllFrom.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            set.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            addTo.addStatement("return $T.this", newTypeName);
            addAllTo.addStatement("return $T.this", newTypeName);
            removeFrom.addStatement("return $T.this", newTypeName);
            removeAllFrom.addStatement("return $T.this", newTypeName);
            set.addStatement("return $T.this", newTypeName);
        }

        // Extra update methods only for non frozen collections
        if (!fieldMetaSignature.context.columnInfo.frozen == true) {
            updateMethods.add(addTo.build());
            updateMethods.add(addAllTo.build());
            updateMethods.add(removeFrom.build());
            updateMethods.add(removeAllFrom.build());
        }

        updateMethods.add(set.build());

        createRelationClassForColumn(parentSignature,
                fieldMetaSignature, newTypeName,
                returnType, updateMethods);
    }

    public void buildMethodsForMapUpdate(ParentSignature parentSignature,
                                         TypeName newTypeName,
                                         FieldMetaSignature fieldMetaSignature,
                                         ReturnType returnType) {
        final String fieldName = parentSignature.parentFieldName
                .map(x -> String.format("%s.udtClassProperty.%s",x, fieldMetaSignature.context.fieldName))
                .orElse(fieldMetaSignature.context.fieldName);

        final String paramKey = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMetaSignature.context.fieldName + "_key")
                .orElse(fieldMetaSignature.context.fieldName + "_key");

        final String paramValue = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMetaSignature.context.fieldName + "_value")
                .orElse(fieldMetaSignature.context.fieldName + "_value");

        final String param = parentSignature.parentFieldName
                .map(x -> x + "_" + fieldMetaSignature.context.fieldName)
                .orElse(fieldMetaSignature.context.fieldName);

        final String cqlColumn = parentSignature.parentQuotedCQLColumn
                .map(x -> x + "." + fieldMetaSignature.context.quotedCqlColumn)
                .orElse(fieldMetaSignature.context.quotedCqlColumn);

        final TypeName sourceType = fieldMetaSignature.sourceType;
        final TypeName nestedKeyType = parentSignature.aptUtils.extractTypeArgument(sourceType, 0);
        final TypeName nestedValueType = parentSignature.aptUtils.extractTypeArgument(sourceType, 1);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder putTo = MethodSpec.methodBuilder("PutTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = ?</strong>", cqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
                .addParameter(nestedValueType, paramValue, Modifier.FINAL)
                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
                .addStatement("boundValues.add($N)", paramKey)
                .addStatement("boundValues.add($N)", paramValue)
                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, paramKey, OPTIONAL)
                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, paramValue, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder("AddAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder removeByKey = MethodSpec.methodBuilder("RemoveByKey")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = null</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
                .addStatement("boundValues.add($N)", paramKey)
                .addStatement("boundValues.add(null)")
                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, paramKey, OPTIONAL)
                .addStatement("encodedValues.add(null)")
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder("Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.of($S, $T.bindMarker($S)))",
                        NON_ESCAPING_ASSIGNMENT, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, param, OPTIONAL)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            putTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            addAllTo.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            removeByKey.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            set.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            putTo.addStatement("return $T.this", newTypeName);
            addAllTo.addStatement("return $T.this", newTypeName);
            removeByKey.addStatement("return $T.this", newTypeName);
            set.addStatement("return $T.this", newTypeName);
        }

        // Extra update methods only for non frozen collections
        if (!fieldMetaSignature.context.columnInfo.frozen == true) {
            updateMethods.add(putTo.build());
            updateMethods.add(addAllTo.build());
            updateMethods.add(removeByKey.build());
        }

        updateMethods.add(set.build());

        createRelationClassForColumn(parentSignature,
                fieldMetaSignature, newTypeName,
                returnType, updateMethods);
    }

    public void buildMethodsForCounterUpdate(ParentSignature parentSignature,
                                             TypeName newTypeName,
                                             FieldMetaSignature parsingResult,
                                             ReturnType returnType) {
        final String fieldName =parsingResult.context.fieldName;
        final String paramIncr = parsingResult.context.fieldName + "_increment";
        final String paramDecr = parsingResult.context.fieldName + "_decrement";
        final String cqlColumn = parsingResult.context.quotedCqlColumn;

        final TypeName sourceType = parsingResult.sourceType;

        List<MethodSpec> updateMethods = new ArrayList<>();

        final MethodSpec.Builder incrOne = MethodSpec.methodBuilder("Incr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + 1</strong>", cqlColumn, cqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.with($T.incr($S))",
                        QUERY_BUILDER, cqlColumn)
                .returns(newTypeName);

        final MethodSpec.Builder incr = MethodSpec.methodBuilder("Incr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, paramIncr, Modifier.FINAL)
                .addStatement("where.with($T.incr($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", paramIncr)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, paramIncr, OPTIONAL)
                .returns(newTypeName);

        final MethodSpec.Builder decrOne = MethodSpec.methodBuilder("Decr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - 1</strong>", fieldName, fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.with($T.decr($S))",
                        QUERY_BUILDER, cqlColumn)
                .returns(newTypeName);

        final MethodSpec.Builder decr = MethodSpec.methodBuilder("Decr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, paramDecr, Modifier.FINAL)
                .addStatement("where.with($T.decr($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", paramDecr)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N, $T.of(cassandraOptions)))", fieldName, paramDecr, OPTIONAL)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            incrOne.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            incr.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            decrOne.addStatement("return new $T(where, cassandraOptions)", newTypeName);
            decr.addStatement("return new $T(where, cassandraOptions)", newTypeName);
        } else {
            incrOne.addStatement("return $T.this", newTypeName);
            incr.addStatement("return $T.this", newTypeName);
            decrOne.addStatement("return $T.this", newTypeName);
            decr.addStatement("return $T.this", newTypeName);
        }

        updateMethods.add(incrOne.build());
        updateMethods.add(incr.build());
        updateMethods.add(decrOne.build());
        updateMethods.add(decr.build());

        createRelationClassForColumn(parentSignature,
                parsingResult, newTypeName,
                returnType, updateMethods);
    }


    public void createRelationClassForColumn(ParentSignature parentSignature, FieldMetaSignature fieldSignature,
                                             TypeName newTypeName, ReturnType returnType, List<MethodSpec> methods) {
        final AptUtils aptUtils = parentSignature.aptUtils;
        final String parentClassName = parentSignature.parentClassName;
        final TypeSpec.Builder parentBuilder = parentSignature.parentBuilder;
        TypeName relationClassTypeName = ClassName.get(DSL_PACKAGE, parentClassName + "." + fieldSignature.relationClassnameForUpdate());
        String fieldName = fieldSignature.context.fieldName;
        final TypeSpec.Builder relationClassBuilder = TypeSpec.classBuilder(fieldSignature.relationClassnameForUpdate())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        methods.forEach(relationClassBuilder::addMethod);

        augmentUpdateRelationClass(ParentSignature.of(aptUtils,
                relationClassBuilder,
                parentClassName + "." + fieldSignature.relationClassnameForUpdate(),
                parentSignature.parentQuotedCQLColumn,
                parentSignature.parentFieldName),
                fieldSignature, newTypeName, returnType);

        final TypeSpec relationClass = relationClassBuilder.build();

        parentBuilder.addType(relationClass);
        parentBuilder.addMethod(MethodSpec.methodBuilder(fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", relationClassTypeName)
                .returns(relationClassTypeName)
                .build());
    }

    public static class ParentSignature {
        public final AptUtils aptUtils;
        public final TypeSpec.Builder parentBuilder;
        public final String parentClassName;
        public final Optional<String> parentQuotedCQLColumn;
        public final Optional<String> parentFieldName;

        private ParentSignature(AptUtils aptUtils, TypeSpec.Builder parentBuilder, String parentClassName, Optional<String> parentQuotedCQLColumn, Optional<String> parentFieldName) {
            this.aptUtils = aptUtils;
            this.parentBuilder = parentBuilder;
            this.parentClassName = parentClassName;
            this.parentQuotedCQLColumn = parentQuotedCQLColumn;
            this.parentFieldName = parentFieldName;
        }

        public static ParentSignature of(AptUtils aptUtils, TypeSpec.Builder parentBuilder, String parentClassName, Optional<String> parentQuotedCQLColumn, Optional<String> parentFieldName) {
            return new ParentSignature(aptUtils, parentBuilder, parentClassName, parentQuotedCQLColumn, parentFieldName);
        }
    }
}
