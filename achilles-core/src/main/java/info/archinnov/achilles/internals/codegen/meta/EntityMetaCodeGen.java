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

package info.archinnov.achilles.internals.codegen.meta;

import static com.squareup.javapoet.TypeName.BOOLEAN;
import static com.squareup.javapoet.TypeName.INT;
import static info.archinnov.achilles.internals.parser.TypeUtils.getRawType;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.parser.validator.BeanValidator.*;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateCorrectKeysOrder;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.*;
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
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.internals.parser.FieldParser.TypeParsingResult;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.type.tuples.Tuple2;

public class EntityMetaCodeGen extends AbstractBeanMetaCodeGen {

    public static final Comparator<Tuple2<String, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            (o1, o2) -> o1._2().order.compareTo(o2._2().order);
    public static final Comparator<Tuple2<String, ClusteringColumnInfo>> CLUSTERING_COLUMN_SORTER =
            (o1, o2) -> o1._2().order.compareTo(o2._2().order);
    public static final Comparator<Tuple2<String, String>> BY_CQL_NAME_COLUMN_SORTER =
            (o1, o2) -> o1._1().compareTo(o2._1());
    private final AptUtils aptUtils;

    public EntityMetaCodeGen(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public EntityMetaSignature buildEntityMeta(EntityType entityType, TypeElement elm, GlobalParsingContext globalParsingContext, List<TypeParsingResult> parsingResults) {
        final TypeName rawClassTypeName = getRawType(TypeName.get(elm.asType()));
        final Optional<Consistency> consistency = aptUtils.getAnnotationOnClass(elm, Consistency.class);
        final Optional<TTL> ttl = aptUtils.getAnnotationOnClass(elm, TTL.class);
        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(elm, Strategy.class);
        final Optional<Entity> entityAnnot = aptUtils.getAnnotationOnClass(elm, Entity.class);

        final Optional<TypeName> viewBaseClass = AnnotationTree.findOptionalViewBaseClass(aptUtils, elm);

        aptUtils.validateFalse(entityAnnot.isPresent() && viewBaseClass.isPresent(),
            "Cannot have both @Entity and @MaterializedView on the class '%s'", rawClassTypeName);

        if (entityType == EntityType.VIEW) {
            aptUtils.validateTrue(viewBaseClass.isPresent(),"Missing @MaterializedView annotation on entity class '%s'", rawClassTypeName);
        }

        validateIsAConcreteNonFinalClass(aptUtils, elm);
        validateHasPublicConstructor(aptUtils, rawClassTypeName, elm);
        validateNoDuplicateNames(aptUtils, rawClassTypeName, parsingResults);
        validateHasPartitionKey(aptUtils, rawClassTypeName, parsingResults);

        final boolean isCounter = BeanValidator.isCounterTable(aptUtils, rawClassTypeName, parsingResults);

        if (entityType == EntityType.TABLE) {
            validateStaticColumns(aptUtils, rawClassTypeName, parsingResults);
        } else if (entityType == EntityType.VIEW) {
            validateNoStaticColumnsForView(aptUtils, rawClassTypeName, parsingResults);
            aptUtils.validateFalse(isCounter, "The class '%s' cannot have counter columns because it is a materialized view", rawClassTypeName);
        }

        validateComputed(aptUtils, rawClassTypeName, parsingResults);
        validateCqlColumnNotReservedWords(aptUtils, rawClassTypeName, parsingResults);

        validateCorrectKeysOrder(aptUtils, rawClassTypeName, parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (KeyColumnInfo) x.context.columnInfo))
                .collect(toList()), "@PartitionKey");

        validateCorrectKeysOrder(aptUtils, rawClassTypeName, parsingResults
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple2.of(x.context.fieldName, (KeyColumnInfo) x.context.columnInfo))
                .collect(toList()), "@ClusteringColumn");


        final TypeName rawBeanType = TypeName.get(aptUtils.erasure(elm));

        final String className = elm.getSimpleName() + META_SUFFIX;
        final TypeName typeName = ClassName.get(TypeUtils.ENTITY_META_PACKAGE, className);

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
                .addMethod(buildFieldNameToCqlColumn(parsingResults))
                .addMethod(buildGetStaticReadConsistency(consistency))
                .addMethod(buildGetStaticNamingStrategy(strategy))
                .addMethod(buildPartitionKeys(parsingResults, rawBeanType))
                .addMethod(buildClusteringColumns(parsingResults, rawBeanType))
                .addMethod(buildNormalColumns(parsingResults, rawBeanType))
                .addMethod(buildComputedColumns(parsingResults, rawBeanType));

        if (entityType == EntityType.TABLE) {
            builder.superclass(genericType(ABSTRACT_ENTITY_PROPERTY, rawBeanType))
                    .addMethod(buildIsCounterTable(isCounter))
                    .addMethod(buildStaticKeyspace(aptUtils.getAnnotationOnClass(elm, Entity.class).get().keyspace()))
                    .addMethod(buildStaticTableOrViewName(aptUtils.getAnnotationOnClass(elm, Entity.class).get().table()))
                    .addMethod(buildGetStaticWriteConsistency(consistency))
                    .addMethod(buildGetStaticSerialConsistency(consistency))
                    .addMethod(buildGetStaticTTL(ttl))
                    .addMethod(buildGetStaticInsertStrategy(strategy))
                    .addMethod(buildStaticColumns(parsingResults, rawBeanType))
                    .addMethod(buildCounterColumns(parsingResults, rawBeanType));
        } else if (entityType == EntityType.VIEW) {
            builder.superclass(genericType(ABSTRACT_VIEW_PROPERTY, rawBeanType))
                    .addMethod(buildStaticKeyspace(aptUtils.getAnnotationOnClass(elm, MaterializedView.class).get().keyspace()))
                    .addMethod(buildStaticTableOrViewName(aptUtils.getAnnotationOnClass(elm, MaterializedView.class).get().view()))
                    .addMethod(buildGetBaseEntityClass(viewBaseClass.get()));
        }

        for(TypeParsingResult x: parsingResults) {
            builder.addField(x.buildPropertyAsField());
        }

        return new EntityMetaSignature(entityType, builder.build(), elm.getSimpleName().toString(), typeName, rawBeanType, viewBaseClass, parsingResults);
    }

    private MethodSpec buildFieldNameToCqlColumn(List<TypeParsingResult> parsingResults) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("fieldNameToCqlColumn")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(BIMAP, STRING, STRING))
                .addStatement("$T<$T,$T> map = $T.create($L)", BIMAP, STRING, STRING, HASHBIMAP, parsingResults.size());

        parsingResults
                .stream()
                .map(x -> Tuple2.of(x.context.fieldName, x.context.cqlColumn))
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

    private MethodSpec buildPartitionKeys(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private MethodSpec buildClusteringColumns(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private MethodSpec buildStaticColumns(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private MethodSpec buildComputedColumns(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private MethodSpec buildCounterColumns(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private MethodSpec buildNormalColumns(List<TypeParsingResult> parsingResults, TypeName rawClassType) {
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

    private ParameterizedTypeName propertyListType(TypeName rawClassType) {
        return genericType(LIST, genericType(ABSTRACT_PROPERTY, rawClassType, WILDCARD, WILDCARD));
    }

    public static class EntityMetaSignature {
        public final TypeSpec sourceCode;
        public final String className;
        public final TypeName typeName;
        public final TypeName entityRawClass;
        public final String fieldName;
        public final List<TypeParsingResult> parsingResults;
        public final EntityType entityType;
        public final Optional<TypeName> viewBaseClass;

        public EntityMetaSignature(EntityType entityType, TypeSpec sourceCode, String className, TypeName typeName, TypeName entityRawClass, Optional<TypeName> viewBaseClass, List<TypeParsingResult> parsingResults) {
            this.entityType = entityType;
            this.sourceCode = sourceCode;
            this.className = className;
            this.typeName = typeName;
            this.entityRawClass = entityRawClass;
            this.viewBaseClass = viewBaseClass;
            this.parsingResults = parsingResults;
            this.fieldName = className.substring(0, 1).toLowerCase() + className.substring(1);
        }

        public boolean hasClustering() {
            return parsingResults.stream().filter(x -> x.context.columnType == CLUSTERING).count() > 0;
        }

        public boolean hasStatic() {
            return parsingResults.stream().filter(x -> x.context.columnType == STATIC || x.context.columnType == STATIC_COUNTER).count() > 0;
        }

        public boolean isCounterEntity() {
            return parsingResults.stream()
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
            return className + suffix + "_" + upperCaseFirst(fieldName);
        }

        public String endClassName(String suffix) {
            return className + suffix;
        }

        public String endReturnType(String dslSuffix, String suffix) {
            return className + dslSuffix + "." + className + suffix;
        }

        public String whereReturnType(String fieldName, String dslSuffix, String suffix) {
            return className + dslSuffix + "." + className + suffix + "_" + upperCaseFirst(fieldName);
        }

        public String selectClassName() {
            return className + SELECT_DSL_SUFFIX;
        }

        public String selectFromReturnType() {
            return selectClassName() + "." + className + SELECT_FROM_DSL_SUFFIX;
        }

        public String selectColumnsReturnType() {
            return selectClassName() + "." + className + SELECT_COLUMNS_DSL_SUFFIX;
        }

        public String selectWhereReturnType(String fieldName) {
            return selectClassName() + "." + className + SELECT_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String selectEndReturnType() {
            return selectClassName() + "." + className + SELECT_END_DSL_SUFFIX;
        }


        public String deleteClassName() {
            return className + DELETE_DSL_SUFFIX;
        }

        public String deleteFromReturnType() {
            return deleteClassName() + "." + className + DELETE_FROM_DSL_SUFFIX;
        }

        public String deleteColumnsReturnType() {
            return deleteClassName() + "." + className + DELETE_COLUMNS_DSL_SUFFIX;
        }

        public String deleteWhereReturnType(String fieldName) {
            return deleteClassName() + "." + className + DELETE_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }



        public String deleteStaticClassName() {
            return className + DELETE_STATIC_DSL_SUFFIX;
        }

        public String deleteStaticFromReturnType() {
            return deleteStaticClassName() + "." + className + DELETE_STATIC_FROM_DSL_SUFFIX;
        }

        public String deleteStaticColumnsReturnType() {
            return deleteStaticClassName() + "." + className + DELETE_STATIC_COLUMNS_DSL_SUFFIX;
        }

        public String deleteStaticWhereReturnType(String fieldName) {
            return deleteStaticClassName() + "." + className + DELETE_STATIC_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String updateClassName() {
            return className + UPDATE_DSL_SUFFIX;
        }

        public String updateFromReturnType() {
            return updateClassName() + "." + className + UPDATE_FROM_DSL_SUFFIX;
        }

        public String updateColumnsReturnType() {
            return updateClassName() + "." + className + UPDATE_COLUMNS_DSL_SUFFIX;
        }

        public String updateWhereReturnType(String fieldName) {
            return updateClassName() + "." + className + UPDATE_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        public String updateStaticClassName() {
            return className + UPDATE_STATIC_DSL_SUFFIX;
        }

        public String updateStaticFromReturnType() {
            return updateStaticClassName() + "." + className + UPDATE_STATIC_FROM_DSL_SUFFIX;
        }

        public String updateStaticColumnsReturnType() {
            return updateStaticClassName() + "." + className + UPDATE_STATIC_COLUMNS_DSL_SUFFIX;
        }

        public String updateStaticWhereReturnType(String fieldName) {
            return updateStaticClassName() + "." + className + UPDATE_STATIC_WHERE_DSL_SUFFIX + "_" + upperCaseFirst(fieldName);
        }

        protected static String upperCaseFirst(String fieldName) {
            return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        }
    }
}
