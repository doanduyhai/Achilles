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

package info.archinnov.achilles.internals.codegen.meta;

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.INT;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.MATERIALIZED_VIEW;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static info.archinnov.achilles.internals.utils.NamingHelper.upperCaseFirst;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.squareup.javapoet.*;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty.EntityType;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.type.tuples.Tuple2;

public class EntityMetaCodeGen implements CommonBeanMetaCodeGen {

    public static final Comparator<Tuple2<String, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            Comparator.comparing(o -> o._2().order);
    public static final Comparator<Tuple2<String, ClusteringColumnInfo>> CLUSTERING_COLUMN_SORTER =
            Comparator.comparing(o -> o._2().order);
    public static final Comparator<Tuple2<String, String>> BY_CQL_NAME_COLUMN_SORTER =
            Comparator.comparing(Tuple2::_1);
    private final AptUtils aptUtils;

    public EntityMetaCodeGen(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public EntityMetaSignature buildEntityMeta(EntityType entityType, TypeElement elm, GlobalParsingContext globalParsingContext,
                                               List<FieldMetaSignature> fieldMetaSignatures,
                                               List<FieldMetaSignature> customConstructorFieldMetaSignatures) {
        final TypeName rawClassTypeName = getRawType(TypeName.get(elm.asType()));
        final Optional<Consistency> consistency = aptUtils.getAnnotationOnClass(elm, Consistency.class);
        final Optional<TTL> ttl = aptUtils.getAnnotationOnClass(elm, TTL.class);
        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(elm, Strategy.class);
        final Optional<Table> entityAnnot = aptUtils.getAnnotationOnClass(elm, Table.class);

        final Optional<TypeName> viewBaseClass = AnnotationTree.findOptionalViewBaseClass(aptUtils, elm);

        aptUtils.validateFalse(entityAnnot.isPresent() && viewBaseClass.isPresent(),
            "Cannot have both @Table and @MaterializedView on the class '%s'", rawClassTypeName);

        if (entityType == EntityType.VIEW) {
            aptUtils.validateTrue(viewBaseClass.isPresent(),"Missing @MaterializedView annotation on entity class '%s'", rawClassTypeName);
        }

        final BeanValidator beanValidator = globalParsingContext.beanValidator();
        final FieldValidator fieldValidator = globalParsingContext.fieldValidator();

        beanValidator.validateIsAConcreteClass(aptUtils, elm);
        beanValidator.validateNoDuplicateNames(aptUtils, rawClassTypeName, fieldMetaSignatures);
        beanValidator.validateHasPartitionKey(aptUtils, rawClassTypeName, fieldMetaSignatures);
        beanValidator.validateConstructor(aptUtils, rawClassTypeName, elm);

        final boolean isCounter = beanValidator.isCounterTable(aptUtils, rawClassTypeName, fieldMetaSignatures);

        if (entityType == EntityType.TABLE) {
            beanValidator.validateStaticColumns(aptUtils, rawClassTypeName, fieldMetaSignatures);
        } else if (entityType == EntityType.VIEW && globalParsingContext.supportsFeature(MATERIALIZED_VIEW)) {
            beanValidator.validateNoStaticColumnsForView(aptUtils, rawClassTypeName, fieldMetaSignatures);
            aptUtils.validateFalse(isCounter, "The class '%s' cannot have counter columns because it is a materialized view", rawClassTypeName);
        }

        beanValidator.validateComputed(aptUtils, rawClassTypeName, fieldMetaSignatures);
        beanValidator.validateCqlColumnNotReservedWords(aptUtils, rawClassTypeName, fieldMetaSignatures);

        fieldValidator.validateCorrectKeysOrder(aptUtils, rawClassTypeName, fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (KeyColumnInfo) x.context.columnInfo))
                .collect(toList()), "@PartitionKey");

        fieldValidator.validateCorrectKeysOrder(aptUtils, rawClassTypeName, fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple2.of(x.context.fieldName, (KeyColumnInfo) x.context.columnInfo))
                .collect(toList()), "@ClusteringColumn");


        final TypeName rawBeanType = TypeName.get(aptUtils.erasure(elm));

        final String className = elm.getSimpleName() + META_SUFFIX;
        final TypeName typeName = ClassName.get(ENTITY_META_PACKAGE, className);

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addJavadoc("Meta class of all entities of type $T<br/>\n", rawBeanType)
                .addJavadoc("The meta class is responsible for<br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>determining runtime consistency levels (read/write,serial)<li/>\n");

        if (entityType == EntityType.TABLE) {
            builder.addJavadoc("   <li>determining runtime insert strategy<li/>\n");
        }

        builder.addJavadoc("   <li>trigger event interceptors (if any)<li/>\n")
               .addJavadoc("   <li>map a $T back to an instance of $T<li/>\n", ClassName.get(Row.class), rawBeanType)
               .addJavadoc("   <li>determine runtime keyspace name using static annotations and runtime SchemaNameProvider (if any)<li/>\n")
               .addJavadoc("   <li>determine runtime table name using static annotations and runtime SchemaNameProvider (if any)<li/>\n")
               .addJavadoc("   <li>generate schema during bootstrap<li/>\n")
               .addJavadoc("   <li>validate schema during bootstrap<li/>\n")
               .addJavadoc("   <li>expose all property meta classes for encoding/decoding purpose on unitary columns<li/>\n")
               .addJavadoc("<ul/>\n");

        builder.addAnnotation(ACHILLES_META_ANNOT)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildEntityClass(rawClassTypeName))
                .addMethod(buildDerivedTableName(elm, globalParsingContext.namingStrategy))
                .addMethod(buildFieldNameToCqlColumn(fieldMetaSignatures))
                .addMethod(buildGetStaticReadConsistency(consistency))
                .addMethod(buildGetStaticNamingStrategy(strategy))
                .addMethod(buildPartitionKeys(fieldMetaSignatures, rawBeanType))
                .addMethod(buildClusteringColumns(fieldMetaSignatures, rawBeanType))
                .addMethod(buildNormalColumns(fieldMetaSignatures, rawBeanType))
                .addMethod(buildComputedColumns(fieldMetaSignatures, rawBeanType))
                .addMethod(buildConstructorInjectedColumns(customConstructorFieldMetaSignatures, rawBeanType));

        if (entityType == EntityType.TABLE) {
            builder.superclass(genericType(ABSTRACT_ENTITY_PROPERTY, rawBeanType))
                    .addMethod(buildIsCounterTable(isCounter))
                    .addMethod(buildStaticKeyspace(aptUtils.getAnnotationOnClass(elm, Table.class).get().keyspace()))
                    .addMethod(buildStaticTableOrViewName(aptUtils.getAnnotationOnClass(elm, Table.class).get().table()))
                    .addMethod(buildGetStaticWriteConsistency(consistency))
                    .addMethod(buildGetStaticSerialConsistency(consistency))
                    .addMethod(buildGetStaticTTL(ttl))
                    .addMethod(buildGetStaticInsertStrategy(strategy))
                    .addMethod(buildStaticColumns(fieldMetaSignatures, rawBeanType))
                    .addMethod(buildCounterColumns(fieldMetaSignatures, rawBeanType));
        } else if (entityType == EntityType.VIEW && globalParsingContext.supportsFeature(MATERIALIZED_VIEW)) {
            builder.superclass(genericType(ABSTRACT_VIEW_PROPERTY, rawBeanType))
                    .addMethod(buildStaticKeyspace(aptUtils.getAnnotationOnClass(elm, MaterializedView.class).get().keyspace()))
                    .addMethod(buildStaticTableOrViewName(aptUtils.getAnnotationOnClass(elm, MaterializedView.class).get().view()))
                    .addMethod(buildGetBaseEntityClass(viewBaseClass.get()));
        }

        builder.addMethod(buildNewInstanceFromCustomConstructor(customConstructorFieldMetaSignatures, rawClassTypeName));

        for(FieldMetaSignature x: fieldMetaSignatures) {
            builder.addField(x.buildPropertyAsField());
        }

        // Build public static final xxx_AchillesMeta.ColumnsForFunctions COLUMNS = new xxx_AchillesMeta.ColumnsForFunctions();
        builder.addType(EntityMetaColumnsForFunctionsCodeGen.createColumnsClassForFunctionParam(fieldMetaSignatures))
                .addField(buildColumnsField(className));

        return new EntityMetaSignature(entityType, builder.build(), elm.getSimpleName().toString(), typeName, rawBeanType, viewBaseClass,
                fieldMetaSignatures, customConstructorFieldMetaSignatures);
    }

    private MethodSpec buildNewInstanceFromCustomConstructor(List<FieldMetaSignature> customConstructorFieldMetaSignatures, TypeName rawClassTypeName) {
        final MethodSpec.Builder methodSpec = MethodSpec
                .methodBuilder("newInstanceFromCustomConstructor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .addParameter(ROW, "row", Modifier.FINAL)
                .addParameter(genericType(LIST, STRING), "cqlColumns", Modifier.FINAL)
                .returns(rawClassTypeName);

        if (customConstructorFieldMetaSignatures.size() > 0) {
            customConstructorFieldMetaSignatures
                    .forEach(field ->
                            methodSpec.addStatement("final $T $L_value = cqlColumns.contains($L.getColumnForSelect()) ? $L.decodeFromGettable(row): null",
                                    field.sourceType.box(),
                                    field.context.fieldName,
                                    field.context.fieldName,
                                    field.context.fieldName)
                    );

            methodSpec.addStatement(customConstructorFieldMetaSignatures
                    .stream()
                    .map(fieldMeta -> fieldMeta.context.fieldName + "_value")
                    .collect(joining(",", "return new $T(", ")")), rawClassTypeName);
        } else {
            final String errorMessage = "Cannot instantiate entity '" + rawClassTypeName.toString() + "' using custom constructor because no custom constructor (@EntityCreator) is defined";
            methodSpec.addStatement("throw new $T($S)", TypeName.get(UnsupportedOperationException.class), errorMessage);
        }

        return methodSpec.build();
    }

    private MethodSpec buildFieldNameToCqlColumn(List<FieldMetaSignature> parsingResults) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("fieldNameToCqlColumn")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(BIMAP, STRING, STRING))
                .addStatement("$T<$T,$T> map = $T.create($L)", BIMAP, STRING, STRING, HASHBIMAP, parsingResults.size());

        parsingResults
                .stream()
                .map(x -> Tuple2.of(x.context.fieldName, x.context.quotedCqlColumn))
                .forEach(x -> builder.addStatement("map.put($S, $S)", x._1(), x._2()));

        builder.addStatement("return map");
        return builder.build();
    }

    private MethodSpec buildEntityClass(TypeName rawClassTypeName) {
        return MethodSpec.methodBuilder("getEntityClass")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(classTypeOf(rawClassTypeName))
                .addStatement("return $T.class", rawClassTypeName)
                .build();
    }

    private MethodSpec buildStaticKeyspace(String staticValue) {

        final Optional<String> keyspace = Optional.ofNullable(isBlank(staticValue) ? null : staticValue);

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticKeyspace")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, STRING));

        if (keyspace.isPresent()) {
            return builder.addStatement("return $T.of($S)", OPTIONAL, keyspace.get()).build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildDerivedTableName(TypeElement elm, InternalNamingStrategy defaultStrategy) {
        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(elm, Strategy.class);

        final String tableName = inferNamingStrategy(strategy, defaultStrategy).apply(elm.getSimpleName().toString());

        return MethodSpec.methodBuilder("getDerivedTableOrViewName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(String.class)
                .addStatement("return $S", tableName)
                .build();

    }

    private MethodSpec buildStaticTableOrViewName(String staticValue) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticTableOrViewName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, STRING));
        final Optional<String> tableName = Optional.ofNullable(StringUtils.isBlank(staticValue) ? null : staticValue);

        if (tableName.isPresent()) {
            return builder.addStatement("return $T.of($S)", OPTIONAL, tableName.get()).build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildPartitionKeys(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .map(x -> x._1())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getPartitionKeys")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildClusteringColumns(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple2.of(x.context.fieldName, (ClusteringColumnInfo) x.context.columnInfo))
                .sorted(CLUSTERING_COLUMN_SORTER)
                .map(x -> x._1())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getClusteringColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildStaticColumns(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> (x.context.columnType == ColumnType.STATIC || x.context.columnType == ColumnType.STATIC_COUNTER))
                .map(x -> Tuple2.of(x.context.cqlColumn, x.context.fieldName))
                .sorted(BY_CQL_NAME_COLUMN_SORTER)
                .map(x -> x._2())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getStaticColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildComputedColumns(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .map(x -> Tuple2.of(((ComputedColumnInfo) x.context.columnInfo).alias, x.context.fieldName))
                .sorted(BY_CQL_NAME_COLUMN_SORTER)
                .map(x -> x._2())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getComputedColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildConstructorInjectedColumns(List<FieldMetaSignature> constructorFieldMetaSignatures, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        constructorFieldMetaSignatures
                .stream()
                .map(x -> x.context.fieldName)
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getConstructorInjectedColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildCounterColumns(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> x.context.columnType == COUNTER)
                .map(x -> Tuple2.of(x.context.cqlColumn, x.context.fieldName))
                .sorted(BY_CQL_NAME_COLUMN_SORTER)
                .map(x -> x._2())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getCounterColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildNormalColumns(List<FieldMetaSignature> parsingResults, TypeName rawClassType) {
        StringJoiner joiner = new StringJoiner(",");
        parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.NORMAL)
                .map(x -> Tuple2.of(x.context.cqlColumn, x.context.fieldName))
                .sorted(BY_CQL_NAME_COLUMN_SORTER)
                .map(x -> x._2())
                .forEach(x -> joiner.add(x));

        return MethodSpec.methodBuilder("getNormalColumns")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassType))
                .addStatement("return $T.asList($L)", ARRAYS, joiner.toString())
                .build();
    }

    private MethodSpec buildIsCounterTable(boolean isCounter) {
        return MethodSpec.methodBuilder("isCounterTable")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(BOOLEAN)
                .addStatement("return $L", isCounter)
                .build();
    }

    private MethodSpec buildGetStaticReadConsistency(Optional<Consistency> consistency) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticReadConsistency")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, CONSISTENCY_LEVEL));

        if (consistency.isPresent()) {
            final ConsistencyLevel consistencyLevel = consistency.get().read();
            aptUtils.validateFalse(consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL,
                    "Static read consistency level cannot be SERIAL or LOCAL_SERIAL");
            return builder
                    .addStatement("return $T.of($T.$L)", OPTIONAL, CONSISTENCY_LEVEL,
                            consistencyLevel.name())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildGetStaticWriteConsistency(Optional<Consistency> consistency) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticWriteConsistency")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, CONSISTENCY_LEVEL));

        if (consistency.isPresent()) {
            final ConsistencyLevel consistencyLevel = consistency.get().write();
            aptUtils.validateFalse(consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL,
                    "Static write consistency level cannot be SERIAL or LOCAL_SERIAL");
            return builder
                    .addStatement("return $T.of($T.$L)", OPTIONAL, CONSISTENCY_LEVEL,
                            consistencyLevel.name())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildGetStaticSerialConsistency(Optional<Consistency> consistency) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticSerialConsistency")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, CONSISTENCY_LEVEL));

        if (consistency.isPresent()) {
            final ConsistencyLevel consistencyLevel = consistency.get().serial();
            aptUtils.validateTrue(consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL,
                    "Static serial consistency level should be SERIAL or LOCAL_SERIAL");
            return builder
                    .addStatement("return $T.of($T.$L)", OPTIONAL, CONSISTENCY_LEVEL,
                            consistencyLevel.name())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildGetStaticTTL(Optional<TTL> ttl) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticTTL")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, INT.box()));

        if (ttl.isPresent()) {
            return builder
                    .addStatement("return $T.of($L)", OPTIONAL, ttl.get().value())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildGetStaticInsertStrategy(Optional<Strategy> strategy) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticInsertStrategy")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, INSERT_STRATEGY));

        if (strategy.isPresent()) {
            return builder
                    .addStatement("return $T.of($T.$L)", OPTIONAL, INSERT_STRATEGY,
                            strategy.get().insert().name())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    private MethodSpec buildGetBaseEntityClass(TypeName baseEntityRawType) {
        return MethodSpec.methodBuilder("getBaseEntityClass")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(genericType(CLASS, WILDCARD))
                .addStatement("return $T.class", baseEntityRawType)
                .build();
    }


    private FieldSpec buildColumnsField(String parentClassName) {

        TypeName typeName = ClassName.get(ENTITY_META_PACKAGE, parentClassName + "." + COLUMNS_FOR_FUNCTIONS_CLASS);
        return FieldSpec
                .builder(typeName, "COLUMNS", Modifier.STATIC, Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Static class to expose $S fields for <strong>type-safe</strong> function calls", parentClassName)
                .initializer(CodeBlock.builder().addStatement("new $T()", typeName).build())
                .build();
    }

    private ParameterizedTypeName propertyListType(TypeName rawClassType) {
        return genericType(LIST, genericType(ABSTRACT_PROPERTY, rawClassType, WILDCARD, WILDCARD));
    }

    public static class EntityMetaSignature {
        public final TypeSpec sourceCode;
        public final String className;
        public final TypeName typeName;
        public final TypeName entityRawClass;
        public final String fieldName;
        public final List<FieldMetaSignature> fieldMetaSignatures;
        public final EntityType entityType;
        public final Optional<TypeName> viewBaseClass;
        public final List<FieldMetaSignature> constructorInjectedFieldMetaSignatures;

        public EntityMetaSignature(EntityType entityType, TypeSpec sourceCode, String className, TypeName typeName, TypeName entityRawClass, Optional<TypeName> viewBaseClass, List<FieldMetaSignature> fieldMetaSignatures, List<FieldMetaSignature> constructorInjectedFieldMetaSignatures) {
            this.entityType = entityType;
            this.sourceCode = sourceCode;
            this.className = className;
            this.typeName = typeName;
            this.entityRawClass = entityRawClass;
            this.viewBaseClass = viewBaseClass;
            this.fieldMetaSignatures = fieldMetaSignatures;
            this.fieldName = className.substring(0, 1).toLowerCase() + className.substring(1);
            this.constructorInjectedFieldMetaSignatures = constructorInjectedFieldMetaSignatures;
        }

        public boolean hasClustering() {
            return fieldMetaSignatures.stream().filter(x -> x.context.columnType == CLUSTERING).count() > 0;
        }

        public boolean hasStatic() {
            return fieldMetaSignatures.stream().filter(x -> x.context.columnType == STATIC || x.context.columnType == STATIC_COUNTER).count() > 0;
        }

        public boolean hasIndex() {
            return fieldMetaSignatures.stream().filter(x -> x.context.indexInfo.type != IndexType.NONE).count() >= 1;
        }

        public boolean isCounterEntity() {
            return fieldMetaSignatures.stream()
                    .filter(x -> x.context.columnType == COUNTER || x.context.columnType == STATIC_COUNTER)
                    .count() > 0;
        }

        public boolean isTable() {
            return entityType == EntityType.TABLE;
        }

        public boolean isView() {
            return entityType == EntityType.VIEW;
        }

        public String whereType(String fieldName, String suffix) {
            return suffix + "_" + upperCaseFirst(fieldName);
        }

        public String endClassName(String suffix) {
            return suffix;
        }

        public String endReturnType(String dslSuffix, String suffix) {
            return className + dslSuffix + "." + suffix;
        }

        public String whereReturnType(String fieldName, String dslSuffix, String suffix) {
            return className + dslSuffix + "." + suffix + "_" + upperCaseFirst(fieldName);
        }

        public String selectClassName() {
            return className + SELECT_DSL_SUFFIX;
        }

        public String indexSelectClassName() {
            return className + INDEX_SELECT_DSL_SUFFIX;
        }

        public String selectFromReturnType() {
            return selectClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String indexSelectFromReturnType() {
            return indexSelectClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String selectFromTypedMapReturnType() {
            return selectClassName() + "." + FROM_TYPED_MAP_DSL_SUFFIX;
        }

        public String indexSelectFromTypedMapReturnType() {
            return indexSelectClassName() + "." + FROM_TYPED_MAP_DSL_SUFFIX;
        }

        public String selectFromJSONReturnType() {
            return selectClassName() + "." + FROM_JSON_DSL_SUFFIX;
        }

        public String indexSelectFromJSONReturnType() {
            return indexSelectClassName() + "." + FROM_JSON_DSL_SUFFIX;
        }

        public String selectColumnsReturnType() {
            return selectClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String indexSelectColumnsReturnType() {
            return indexSelectClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String selectColumnsTypedMapReturnType() {
            return selectClassName() + "." + COLUMNS_TYPED_MAP_DSL_SUFFIX;
        }

        public String indexSelectColumnsTypedMapReturnType() {
            return indexSelectClassName() + "." + COLUMNS_TYPED_MAP_DSL_SUFFIX;
        }

        public String selectWhereReturnType(String fieldName) {
            return selectClassName() + "." + WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String indexSelectWhereReturnType() {
            return indexSelectClassName() + "." + WHERE_DSL_SUFFIX;
        }

        public String selectWhereTypedMapReturnType(String fieldName) {
            return selectClassName() + "." + WHERE_TYPED_MAP_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String indexSelectWhereTypedMapReturnType() {
            return indexSelectClassName() + "." + WHERE_TYPED_MAP_DSL_SUFFIX;
        }

        public String selectWhereJSONReturnType(String fieldName) {
            return selectClassName() + "." + WHERE_JSON_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String indexSelectWhereJSONReturnType() {
            return indexSelectClassName() + "." + WHERE_JSON_DSL_SUFFIX;
        }

        public String selectEndReturnType() {
            return selectClassName() + "." + END_DSL_SUFFIX;
        }

        public String indexSelectEndReturnType() {
            return indexSelectClassName() + "." + END_DSL_SUFFIX;
        }

        public String selectEndTypedMapReturnType() {
            return selectClassName() + "." + END_TYPED_MAP_DSL_SUFFIX;
        }

        public String indexSelectEndTypedMapReturnType() {
            return indexSelectClassName() + "." + END_TYPED_MAP_DSL_SUFFIX;
        }

        public String selectEndJSONReturnType() {
            return selectClassName() + "." + END_JSON_DSL_SUFFIX;
        }

        public String indexSelectEndJSONReturnType() {
            return indexSelectClassName() + "." + END_JSON_DSL_SUFFIX;
        }

        public String deleteClassName() {
            return className + DELETE_DSL_SUFFIX;
        }

        public String deleteFromReturnType() {
            return deleteClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String deleteColumnsReturnType() {
            return deleteClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String deleteWhereReturnType(String fieldName) {
            return deleteClassName() + "." + WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }



        public String deleteStaticClassName() {
            return className + DELETE_STATIC_DSL_SUFFIX;
        }

        public String deleteStaticFromReturnType() {
            return deleteStaticClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String deleteStaticColumnsReturnType() {
            return deleteStaticClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String deleteStaticWhereReturnType(String fieldName) {
            return deleteStaticClassName() + "." + WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String updateClassName() {
            return className + UPDATE_DSL_SUFFIX;
        }

        public String updateFromReturnType() {
            return updateClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String updateColumnsReturnType() {
            return updateClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String updateWhereReturnType(String fieldName) {
            return updateClassName() + "." + WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String updateStaticClassName() {
            return className + UPDATE_STATIC_DSL_SUFFIX;
        }

        public String updateStaticFromReturnType() {
            return updateStaticClassName() + "." + FROM_DSL_SUFFIX;
        }

        public String updateStaticColumnsReturnType() {
            return updateStaticClassName() + "." + COLUMNS_DSL_SUFFIX;
        }

        public String updateStaticWhereReturnType(String fieldName) {
            return updateStaticClassName() + "." + WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }
    }
}
