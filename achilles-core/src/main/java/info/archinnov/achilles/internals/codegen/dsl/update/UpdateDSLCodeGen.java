/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.FieldParser.TypeParsingResult;
import info.archinnov.achilles.type.tuples.Tuple2;

public class UpdateDSLCodeGen extends AbstractDSLCodeGen {


    public static Comparator<Tuple2<String, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            (o1, o2) -> o1._2().order.compareTo(o2._2().order);

    public static TypeSpec buildUpdateClass(AptUtils aptUtils, EntityMetaSignature signature) {

        final String firstPartitionKey = signature.parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        String updateClassName = signature.className + UPDATE_DSL_SUFFIX;
        String updateFromClassName = signature.className + UPDATE_FROM_DSL_SUFFIX;
        TypeName updateFromTypeName = ClassName.get(DSL_PACKAGE, updateFromClassName);

        String updateColumnsClassName = signature.className + UPDATE_COLUMNS_DSL_SUFFIX;
        TypeName updateColumnsTypeName = ClassName.get(DSL_PACKAGE, updateColumnsClassName);

        String updateWhereClassName = signature.className + UPDATE_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(firstPartitionKey);
        TypeName updateWhereTypeName = ClassName.get(DSL_PACKAGE, updateWhereClassName);

        final List<ColumnType> candidateColumns = Arrays.asList(NORMAL, STATIC, COUNTER, STATIC_COUNTER);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateClassName)
                .superclass(ABSTRACT_UPDATE)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildUpdateConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addMethod(buildFromBaseTableMethod(updateFromTypeName))
                .addMethod(buildFromSchemaProviderMethod(updateFromTypeName))
                .addType(buildUpdateColumns(aptUtils, signature, updateColumnsClassName, updateColumnsTypeName,
                        updateWhereTypeName, candidateColumns))
                .addType(buildUpdateFrom(aptUtils, signature, updateFromClassName, updateColumnsTypeName,
                        candidateColumns));


        UpdateWhereDSLCodeGen.buildWhereClasses(signature).forEach(builder::addType);

        return builder.build();
    }

    public static TypeSpec buildUpdateStaticClass(AptUtils aptUtils, EntityMetaSignature signature) {

        final String firstPartitionKey = signature.parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        String updateStaticClassName = signature.className + UPDATE_STATIC_DSL_SUFFIX;
        String updateStaticFromClassName = signature.className + UPDATE_STATIC_FROM_DSL_SUFFIX;
        TypeName updateStaticFromTypeName = ClassName.get(DSL_PACKAGE, updateStaticFromClassName);

        String updateStaticColumnsClassName = signature.className + UPDATE_STATIC_COLUMNS_DSL_SUFFIX;
        TypeName updateStaticColumnsTypeName = ClassName.get(DSL_PACKAGE, updateStaticColumnsClassName);

        String updateStaticWhereClassName = signature.className + UPDATE_STATIC_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(firstPartitionKey);
        TypeName updateStaticWhereTypeName = ClassName.get(DSL_PACKAGE, updateStaticWhereClassName);

        final List<ColumnType> candidateColumns = Arrays.asList(STATIC, STATIC_COUNTER);
        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateStaticClassName)
                .superclass(ABSTRACT_UPDATE)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildUpdateConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addMethod(buildFromBaseTableMethod(updateStaticFromTypeName))
                .addMethod(buildFromSchemaProviderMethod(updateStaticFromTypeName))
                .addType(buildUpdateColumns(aptUtils, signature, updateStaticColumnsClassName, updateStaticColumnsTypeName,
                        updateStaticWhereTypeName, candidateColumns))
                .addType(buildUpdateFrom(aptUtils, signature, updateStaticFromClassName,
                        updateStaticColumnsTypeName, candidateColumns));


        UpdateWhereDSLCodeGen.buildWhereClassesForStatic(signature).forEach(builder::addType);

        return builder.build();
    }

    private static MethodSpec buildUpdateConstructor(EntityMetaSignature signature) {
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

    private static MethodSpec buildFromSchemaProviderMethod(TypeName updateFromTypeName) {
        return MethodSpec.methodBuilder("from")
                .addJavadoc("Generate an UPDATE <strong>FROM</strong> ... using the given SchemaNameProvider")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $T.update(currentKeyspace, currentTable).where()", UPDATE_WHERE, QUERY_BUILDER)
                .addStatement("return new $T(where)", updateFromTypeName)
                .returns(updateFromTypeName)
                .build();
    }

    private static MethodSpec buildFromBaseTableMethod(TypeName updateFromTypeName) {
        return MethodSpec.methodBuilder("fromBaseTable")
                .addJavadoc("Generate an UPDATE <strong>FROM</strong> ...")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final String currentKeyspace = meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName())",
                        "unknown_keyspace_for_")
                .addStatement("final $T where = $T.update(currentKeyspace, meta.getTableName()).where()", UPDATE_WHERE, QUERY_BUILDER)
                .addStatement("return new $T(where)", updateFromTypeName)
                .returns(updateFromTypeName)
                .build();
    }

    private static TypeSpec buildUpdateFrom(AptUtils aptUtils, EntityMetaSignature signature,
                                            String updateFromClassName,
                                            TypeName updateColumnsTypeName,
                                            List<ColumnType> candidateColumns) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateFromClassName)
                .superclass(ABSTRACT_UPDATE_FROM)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(UPDATE_WHERE, "where")
                        .addStatement("super(where)")
                        .build());

        signature.parsingResults
                .stream()
                .filter(x -> candidateColumns.contains(x.context.columnType))
                .forEach(x -> buildUpdateColumnMethods(aptUtils, updateColumnsTypeName, x, ReturnType.NEW)
                        .forEach(builder::addMethod));

        return builder.build();
    }

    private static TypeSpec buildUpdateColumns(AptUtils aptUtils, EntityMetaSignature signature,
                                               String updateColumnsClassName,
                                               TypeName updateColumnsTypeName,
                                               TypeName updateWhereTypeName,
                                               List<ColumnType> candidateColumns) {


        final TypeSpec.Builder builder = TypeSpec.classBuilder(updateColumnsClassName)
                .superclass(ABSTRACT_UPDATE_COLUMNS)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(UPDATE_WHERE, "where")
                        .addStatement("super(where)")
                        .build());

        signature.parsingResults
                .stream()
                .filter(x -> candidateColumns.contains(x.context.columnType))
                .forEach(x -> buildUpdateColumnMethods(aptUtils, updateColumnsTypeName, x, ReturnType.THIS)
                        .forEach(builder::addMethod));

        builder.addMethod(MethodSpec.methodBuilder("where")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(where)", updateWhereTypeName)
                .returns(updateWhereTypeName)
                .build());

        return builder.build();

    }

    private static List<MethodSpec> buildUpdateColumnMethods(AptUtils aptUtils, TypeName nextTypeName,
                                                             TypeParsingResult parsingResult,
                                                             ReturnType returnType) {

        final ColumnType columnType = parsingResult.context.columnType;
        final boolean isCounterColumn = columnType == COUNTER || columnType == STATIC_COUNTER;

        final TypeName rawTargetType = aptUtils.getRawType(parsingResult.targetType);

        if (rawTargetType.equals(LIST)) {
            return buildMethodsForListUpdate(aptUtils, nextTypeName, parsingResult, returnType);
        } else if (rawTargetType.equals(SET)) {
            return buildMethodsForSetUpdate(aptUtils, nextTypeName, parsingResult, returnType);
        } else if (rawTargetType.equals(MAP)) {
            return buildMethodsForMapUpdate(aptUtils, nextTypeName, parsingResult, returnType);
        } else if (isCounterColumn) {
            return buildMethodsForCounterUpdate(nextTypeName, parsingResult, returnType);
        } else {
            return Arrays.asList(buildMethodForSimpleUpdate(nextTypeName, parsingResult, returnType));
        }

    }

    private static MethodSpec buildMethodForSimpleUpdate(TypeName newTypeName, TypeParsingResult parsingResult,
                                                         ReturnType returnType) {
        final String fieldName = parsingResult.context.fieldName;
        final String cqlColumn = parsingResult.context.cqlColumn;
        final TypeName sourceType = parsingResult.sourceType;

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(fieldName + "_Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName);

        if (returnType == ReturnType.NEW) {
            builder.addStatement("return new $T(where)", newTypeName);
        } else {
            builder.addStatement("return this");

        }

        return builder.returns(newTypeName).build();
    }

    private static List<MethodSpec> buildMethodsForListUpdate(AptUtils aptUtils, TypeName newTypeName,
                                                              TypeParsingResult parsingResult,
                                                              ReturnType returnType) {
        final String fieldName = parsingResult.context.fieldName;
        final String param = fieldName + "_element";
        final String cqlColumn = parsingResult.context.cqlColumn;
        final TypeName sourceType = parsingResult.sourceType;
        final TypeName nestedType = aptUtils.extractTypeArgument(sourceType, 0);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder appendTo = MethodSpec.methodBuilder(fieldName + "_AppendTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + [?]</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
                .returns(newTypeName);


        final MethodSpec.Builder appendAllTo = MethodSpec.methodBuilder(fieldName + "_AppendAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.appendAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);


        final MethodSpec.Builder prependTo = MethodSpec.methodBuilder(fieldName + "_PrependTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = [?] + $L</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
                .returns(newTypeName);


        final MethodSpec.Builder prependAllTo = MethodSpec.methodBuilder(fieldName + "_PrependAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ? + $L</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.prependAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        final MethodSpec.Builder setAtIndex = MethodSpec.methodBuilder(fieldName + "_SetAtIndex")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = ?</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT, "index", Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N))", fieldName, param)
                .returns(newTypeName);

        final MethodSpec.Builder removeAtIndex = MethodSpec.methodBuilder(fieldName + "_RemoveAtIndex")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[index] = null</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(TypeName.INT, "index", Modifier.FINAL)
                .addStatement("where.with($T.setIdx($S, index, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add(null)")
                .addStatement("encodedValues.add(null)")
                .returns(newTypeName);

        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder(fieldName + "_RemoveFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - [?]</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.asList($N))", ARRAYS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.asList($N)))", fieldName, ARRAYS, param)
                .returns(newTypeName);

        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder(fieldName + "_RemoveAllFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.discardAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder(fieldName + "_Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            appendTo.addStatement("return new $T(where)", newTypeName);
            appendAllTo.addStatement("return new $T(where)", newTypeName);
            prependTo.addStatement("return new $T(where)", newTypeName);
            prependAllTo.addStatement("return new $T(where)", newTypeName);
            setAtIndex.addStatement("return new $T(where)", newTypeName);
            removeAtIndex.addStatement("return new $T(where)", newTypeName);
            removeFrom.addStatement("return new $T(where)", newTypeName);
            removeAllFrom.addStatement("return new $T(where)", newTypeName);
            set.addStatement("return new $T(where)", newTypeName);
        } else {
            appendTo.addStatement("return this");
            appendAllTo.addStatement("return this");
            prependTo.addStatement("return this");
            prependAllTo.addStatement("return this");
            setAtIndex.addStatement("return this");
            removeAtIndex.addStatement("return this");
            removeFrom.addStatement("return this");
            removeAllFrom.addStatement("return this");
            set.addStatement("return this");
        }

        updateMethods.add(appendTo.build());
        updateMethods.add(appendAllTo.build());
        updateMethods.add(prependTo.build());
        updateMethods.add(prependAllTo.build());
        updateMethods.add(setAtIndex.build());
        updateMethods.add(removeAtIndex.build());
        updateMethods.add(removeFrom.build());
        updateMethods.add(removeAllFrom.build());
        updateMethods.add(set.build());

        return updateMethods;
    }

    private static List<MethodSpec> buildMethodsForSetUpdate(AptUtils aptUtils, TypeName newTypeName,
                                                             TypeParsingResult parsingResult,
                                                             ReturnType returnType) {
        final String fieldName = parsingResult.context.fieldName;
        final String param = fieldName + "_element";
        final String cqlColumn = parsingResult.context.cqlColumn;
        final TypeName sourceType = parsingResult.sourceType;
        final TypeName nestedType = aptUtils.extractTypeArgument(sourceType, 0);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder addTo = MethodSpec.methodBuilder(fieldName + "_AddTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + {?}</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N)))", fieldName, SETS, param)
                .returns(newTypeName);

        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder(fieldName + "_AddAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        final MethodSpec.Builder removeFrom = MethodSpec.methodBuilder(fieldName + "_RemoveFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - {?}</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedType, param, Modifier.FINAL)
                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($T.newHashSet($N))", SETS, param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($T.newHashSet($N)))", fieldName, SETS, param)
                .returns(newTypeName);

        final MethodSpec.Builder removeAllFrom = MethodSpec.methodBuilder(fieldName + "_RemoveAllFrom")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.removeAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder(fieldName + "_Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            addTo.addStatement("return new $T(where)", newTypeName);
            addAllTo.addStatement("return new $T(where)", newTypeName);
            removeFrom.addStatement("return new $T(where)", newTypeName);
            removeAllFrom.addStatement("return new $T(where)", newTypeName);
            set.addStatement("return new $T(where)", newTypeName);
        } else {
            addTo.addStatement("return this");
            addAllTo.addStatement("return this");
            removeFrom.addStatement("return this");
            removeAllFrom.addStatement("return this");
            set.addStatement("return this");
        }

        updateMethods.add(addTo.build());
        updateMethods.add(addAllTo.build());
        updateMethods.add(removeFrom.build());
        updateMethods.add(removeAllFrom.build());
        updateMethods.add(set.build());

        return updateMethods;
    }

    private static List<MethodSpec> buildMethodsForMapUpdate(AptUtils aptUtils, TypeName newTypeName,
                                                             TypeParsingResult parsingResult,
                                                             ReturnType returnType) {
        final String fieldName = parsingResult.context.fieldName;
        final String paramKey = fieldName + "_key";
        final String paramValue = fieldName + "_value";
        final String cqlColumn = parsingResult.context.cqlColumn;
        final TypeName sourceType = parsingResult.sourceType;
        final TypeName nestedKeyType = aptUtils.extractTypeArgument(sourceType, 0);
        final TypeName nestedValueType = aptUtils.extractTypeArgument(sourceType, 1);

        List<MethodSpec> updateMethods = new ArrayList<>();
        final MethodSpec.Builder putTo = MethodSpec.methodBuilder(fieldName + "_PutTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = ?</strong>", fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
                .addParameter(nestedValueType, paramValue, Modifier.FINAL)
                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
                .addStatement("boundValues.add($N)", paramKey)
                .addStatement("boundValues.add($N)", paramValue)
                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N))", fieldName, paramKey)
                .addStatement("encodedValues.add(meta.$L.valueProperty.encodeFromJava($N))", fieldName, paramValue)
                .returns(newTypeName);

        final MethodSpec.Builder addAllTo = MethodSpec.methodBuilder(fieldName + "_AddAllTo")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.addAll($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        final MethodSpec.Builder removeByKey = MethodSpec.methodBuilder(fieldName + "_RemoveByKey")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L[?] = null</strong>", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addParameter(nestedKeyType, paramKey, Modifier.FINAL)
                .addStatement("where.with($T.put($S, $T.bindMarker($S), $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, paramKey, QUERY_BUILDER, paramValue)
                .addStatement("boundValues.add($N)", paramKey)
                .addStatement("boundValues.add(null)")
                .addStatement("encodedValues.add(meta.$L.keyProperty.encodeFromJava($N))", fieldName, paramKey)
                .addStatement("encodedValues.add(null)")
                .returns(newTypeName);

        final MethodSpec.Builder set = MethodSpec.methodBuilder(fieldName + "_Set")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = ?", fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, fieldName, Modifier.FINAL)
                .addStatement("where.with($T.set($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, fieldName)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            putTo.addStatement("return new $T(where)", newTypeName);
            addAllTo.addStatement("return new $T(where)", newTypeName);
            removeByKey.addStatement("return new $T(where)", newTypeName);
            set.addStatement("return new $T(where)", newTypeName);
        } else {
            putTo.addStatement("return this");
            addAllTo.addStatement("return this");
            removeByKey.addStatement("return this");
            set.addStatement("return this");
        }

        updateMethods.add(putTo.build());
        updateMethods.add(addAllTo.build());
        updateMethods.add(removeByKey.build());
        updateMethods.add(set.build());

        return updateMethods;
    }

    private static List<MethodSpec> buildMethodsForCounterUpdate(TypeName newTypeName,
                                                                 TypeParsingResult parsingResult,
                                                                 ReturnType returnType) {
        final String fieldName = parsingResult.context.fieldName;
        final String param = fieldName + "_increment";
        final String cqlColumn = parsingResult.context.cqlColumn;
        final TypeName sourceType = parsingResult.sourceType;

        List<MethodSpec> updateMethods = new ArrayList<>();

        final MethodSpec.Builder incrOne = MethodSpec.methodBuilder(fieldName + "_Incr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + 1</strong>", fieldName, fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.with($T.incr($S))",
                        QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add(1L)")
                .addStatement("encodedValues.add(1L)")
                .returns(newTypeName);

        final MethodSpec.Builder incr = MethodSpec.methodBuilder(fieldName + "_Incr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L + ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.incr($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, param)
                .returns(newTypeName);

        final MethodSpec.Builder decrOne = MethodSpec.methodBuilder(fieldName + "_Decr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - 1</strong>", fieldName, fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("where.with($T.decr($S))",
                        QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add(1L)")
                .addStatement("encodedValues.add(1L)")
                .returns(newTypeName);

        final MethodSpec.Builder decr = MethodSpec.methodBuilder(fieldName + "_Decr")
                .addJavadoc("Generate an UPDATE FROM ... <strong>SET $L = $L - ?</strong>", fieldName, fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(sourceType, param, Modifier.FINAL)
                .addStatement("where.with($T.decr($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, cqlColumn, QUERY_BUILDER, cqlColumn)
                .addStatement("boundValues.add($N)", param)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldName, param)
                .returns(newTypeName);

        if (returnType == ReturnType.NEW) {
            incrOne.addStatement("return new $T(where)", newTypeName);
            incr.addStatement("return new $T(where)", newTypeName);
            decrOne.addStatement("return new $T(where)", newTypeName);
            decr.addStatement("return new $T(where)", newTypeName);
        } else {
            incrOne.addStatement("return this");
            incr.addStatement("return this");
            decrOne.addStatement("return this");
            decr.addStatement("return this");
        }

        updateMethods.add(incrOne.build());
        updateMethods.add(incr.build());
        updateMethods.add(decrOne.build());
        updateMethods.add(decr.build());

        return updateMethods;
    }

}
