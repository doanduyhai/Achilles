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

package info.archinnov.achilles.internals.codegen.dsl;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple4;


public abstract class AbstractDSLCodeGen {

    protected static final String EQ = "eq";
    protected static final String GT = "gt";
    protected static final String GTE = "gte";
    protected static final String LT = "lt";
    protected static final String LTE = "lte";
    public static Comparator<Tuple2<String, PartitionKeyInfo>> TUPLE2_PARTITION_KEY_SORTER =
            (o1, o2) -> o1._2().order.compareTo(o2._2().order);
    public static Comparator<Tuple4<String, String, TypeName, PartitionKeyInfo>> TUPLE4_PARTITION_KEY_SORTER =
            (o1, o2) -> o1._4().order.compareTo(o2._4().order);
    public static Comparator<Tuple4<String, String, TypeName, ClusteringColumnInfo>> TUPLE4_CLUSTERING_COLUMN_SORTER =
            (o1, o2) -> o1._4().order.compareTo(o2._4().order);

    protected static String upperCaseFirst(String fieldName) {
        return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    protected static List<ClassSignatureInfo> buildClassesSignatureForWhereClause(EntityMetaSignature signature,
                                                                                  ClassSignatureParams classSignatureParams,
                                                                                  List<FieldSignatureInfo> partitionKeys,
                                                                                  List<FieldSignatureInfo> clusteringColumns,
                                                                                  WhereClauseFor whereClauseFor) {
        final List<ClassSignatureInfo> signatures = new ArrayList<>();

        for(FieldSignatureInfo x: partitionKeys) {
            final String className = signature.whereType(x.fieldName, classSignatureParams.whereDslSuffix);
            final TypeName typeName = ClassName.get(DSL_PACKAGE, className);
            final TypeName returnTypeName = ClassName.get(DSL_PACKAGE, signature.whereReturnType(x.fieldName,
                    classSignatureParams.dslSuffix, classSignatureParams.whereDslSuffix));
            signatures.add(ClassSignatureInfo.of(typeName, returnTypeName,
                    classSignatureParams.abstractWherePartitionType, className));
        }

        if (whereClauseFor == WhereClauseFor.NORMAL) {
            for (FieldSignatureInfo x : clusteringColumns) {
                final String className = signature.whereType(x.fieldName, classSignatureParams.whereDslSuffix);
                final TypeName typeName = ClassName.get(DSL_PACKAGE, className);
                final TypeName returnTypeName = ClassName.get(DSL_PACKAGE, signature.whereReturnType(x.fieldName,
                        classSignatureParams.dslSuffix, classSignatureParams.whereDslSuffix));
                final TypeName superType = classSignatureParams.abstractEndType.isPresent()
                        ? classSignatureParams.abstractWhereType
                        : genericType(classSignatureParams.abstractWhereType, returnTypeName, signature.entityRawClass);

                signatures.add(ClassSignatureInfo.of(typeName, returnTypeName, superType, className));
            }
        }

        final String endClassName = signature.endClassName(classSignatureParams.endDslSuffix);
        final TypeName endTypeName = ClassName.get(DSL_PACKAGE, endClassName);
        final TypeName endReturnTypeName = ClassName.get(DSL_PACKAGE, signature.endReturnType(classSignatureParams.dslSuffix,
                classSignatureParams.endDslSuffix));
        final ClassName abstractEndType = classSignatureParams.abstractEndType.orElse(classSignatureParams.abstractWhereType);

        signatures.add(ClassSignatureInfo.of(endTypeName, endReturnTypeName, genericType(abstractEndType, endReturnTypeName, signature.entityRawClass),
                endClassName));

        return signatures;
    }

    protected static MethodSpec buildWhereConstructor(TypeName whereType) {
        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(whereType, "where")
                .addStatement("super(where)")
                .build();

    }

    protected static MethodSpec buildColumnRelation(String relation, TypeName nextType, FieldSignatureInfo fieldInfo) {
        final String methodName = fieldInfo.fieldName + "_" + upperCaseFirst(relation);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L $L ?</strong>", fieldInfo.cqlColumn, relationToSymbolForJavaDoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldInfo.typeName, fieldInfo.fieldName)
                .addStatement("where.and($T.$L($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, relation, fieldInfo.cqlColumn, QUERY_BUILDER, methodName)
                .addStatement("boundValues.add($N)", fieldInfo.fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldInfo.fieldName, fieldInfo.fieldName)
                .returns(nextType);

        return builder.addStatement("return new $T(where)", nextType).build();
    }

    protected static MethodSpec buildColumnInVarargs(TypeName nextType, FieldSignatureInfo fieldInfo) {
        final String methodName = fieldInfo.fieldName + "_IN";
        final String param = fieldInfo.fieldName;
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate a SELECT ... FROM ... WHERE ... <strong>$L IN ?</strong>", fieldInfo.cqlColumn)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(ArrayTypeName.of(fieldInfo.typeName), fieldInfo.fieldName)
                .varargs()
                .addStatement("$T.validateTrue($T.isNotEmpty($L), \"Varargs for field '%s' should not be null/empty\", $S)",
                        VALIDATOR, ARRAYS_UTILS, fieldInfo.fieldName, fieldInfo.fieldName)
                .addStatement("where.and($T.in($S,$T.bindMarker($S)))",
                        QUERY_BUILDER, fieldInfo.cqlColumn, QUERY_BUILDER, fieldInfo.cqlColumn)
                .addStatement("final $T varargs = $T.<Object>asList((Object[])$L)", LIST_OBJECT, ARRAYS, param)
                .addStatement("final $T encodedVarargs = $T.<$T>stream(($T[])$L).map(x -> meta.$L.encodeFromJava(x)).collect($T.toList())",
                        LIST_OBJECT, ARRAYS, fieldInfo.typeName, fieldInfo.typeName, fieldInfo.fieldName, fieldInfo.fieldName, COLLECTORS)
                .addStatement("boundValues.add(varargs)")
                .addStatement("encodedValues.add(encodedVarargs)")
                .returns(nextType);
        return builder.addStatement("return new $T(where)", nextType).build();
    }

    protected static MethodSpec buildGetThis(TypeName currentType) {
        return MethodSpec
                .methodBuilder("getThis")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .returns(currentType)
                .addStatement("return this")
                .build();
    }

    protected static MethodSpec buildGetMetaInternal(TypeName currentType) {

        return MethodSpec
                .methodBuilder("getMetaInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return meta")
                .returns(genericType(ABSTRACT_ENTITY_PROPERTY, currentType))
                .build();
    }

    protected static MethodSpec buildGetEntityClass(EntityMetaSignature signature) {
        return MethodSpec
                .methodBuilder("getEntityClass")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return entityClass")
                .returns(classTypeOf(signature.entityRawClass))
                .build();
    }

    protected static MethodSpec buildGetRte() {
        return MethodSpec
                .methodBuilder("getRte")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return rte")
                .returns(RUNTIME_ENGINE)
                .build();
    }

    protected static MethodSpec buildGetOptions() {
        return MethodSpec
                .methodBuilder("getOptions")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return options")
                .returns(OPTIONS)
                .build();
    }

    protected static MethodSpec buildGetBoundValuesInternal() {
        return MethodSpec
                .methodBuilder("getBoundValuesInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return boundValues")
                .returns(LIST_OBJECT)
                .build();
    }

    protected static MethodSpec buildGetEncodedBoundValuesInternal() {
        return MethodSpec
                .methodBuilder("getEncodedValuesInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return encodedValues")
                .returns(LIST_OBJECT)
                .build();
    }

    protected static boolean hasCounter(EntityMetaSignature signature) {
        return signature
                .parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COUNTER || x.context.columnType == ColumnType.STATIC_COUNTER)
                .count() > 0;
    }

    protected static FieldSpec buildExactEntityMetaField(EntityMetaSignature signature) {
        String entityMetaClassName = signature.className + META_SUFFIX;
        TypeName entityMetaExactType = ClassName.get(ENTITY_META_PACKAGE, entityMetaClassName);
        return FieldSpec.builder(entityMetaExactType, "meta", Modifier.FINAL, Modifier.PROTECTED).build();
    }

    protected static FieldSpec buildEntityClassField(EntityMetaSignature signature) {
        return FieldSpec.builder(classTypeOf(signature.entityRawClass), "entityClass", Modifier.FINAL, Modifier.PROTECTED)
                .initializer("$T.class", signature.entityRawClass)
                .build();
    }

    protected static MethodSpec buildAllColumns(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumns_FromBaseTable")
                .addJavadoc("Generate ... * FROM ...")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final $T where = $L.all().from(meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName()), meta.getTableOrViewName()).where()",
                        whereTypeName, privateFieldName, "unknown_keyspace_for_")
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

    protected static MethodSpec buildAllColumnsWithSchemaProvider(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumns_From")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate ... * FROM ... using the given SchemaNameProvider")
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $L.all().from(currentKeyspace, currentTable).where()", whereTypeName, privateFieldName)
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

    protected static MethodSpec buildFrom(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("fromBaseTable")
                .addJavadoc("Generate a ... <strong>FROM xxx</strong> ... ")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final $T where = $L.from(meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName()), " +
                        "meta.getTableOrViewName()).where()", whereTypeName, privateFieldName, "unknown_keyspace_for_")
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

    protected static MethodSpec buildFromWithSchemaProvider(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("from")
                .addJavadoc("Generate a ... <strong>FROM xxx</strong> ... using the given SchemaNameProvider")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $L.from(currentKeyspace, currentTable).where()", whereTypeName, privateFieldName)
                .addStatement("return new $T(where)", newTypeName)
                .returns(newTypeName)
                .build();
    }

    protected static List<FieldSignatureInfo> getPartitionKeysSignatureInfo(List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple4.of(x.context.fieldName, x.context.cqlColumn, x.sourceType, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(TUPLE4_PARTITION_KEY_SORTER)
                .map(x -> FieldSignatureInfo.of(x._1(), x._2(), x._3()))
                .collect(toList()));
    }

    protected static List<FieldSignatureInfo> getClusteringColsSignatureInfo(List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.CLUSTERING)
                .map(x -> Tuple4.of(x.context.fieldName, x.context.cqlColumn, x.sourceType, (ClusteringColumnInfo) x.context.columnInfo))
                .sorted(TUPLE4_CLUSTERING_COLUMN_SORTER)
                .map(x -> FieldSignatureInfo.of(x._1(), x._2(), x._3()))
                .collect(toList()));
    }

    protected static void buildLWtConditionMethods(EntityMetaSignature signature, ClassSignatureInfo currentSignature, boolean hasCounter, TypeSpec.Builder builder) {
        if (!hasCounter) {
            signature.parsingResults.stream()
                    .filter(x -> x.context.columnType == ColumnType.NORMAL || x.context.columnType == ColumnType.STATIC)
                    .forEach(x -> {
                        final FieldSignatureInfo fieldSignatureInfo = FieldSignatureInfo.of(x.context.fieldName, x.context.cqlColumn, x.sourceType);
                        builder.addMethod(buildLWTConditionOnColumn(EQ, fieldSignatureInfo, currentSignature.returnClassType));
                        builder.addMethod(buildLWTConditionOnColumn(GT, fieldSignatureInfo, currentSignature.returnClassType));
                        builder.addMethod(buildLWTConditionOnColumn(GTE, fieldSignatureInfo, currentSignature.returnClassType));
                        builder.addMethod(buildLWTConditionOnColumn(LT, fieldSignatureInfo, currentSignature.returnClassType));
                        builder.addMethod(buildLWTConditionOnColumn(LTE, fieldSignatureInfo, currentSignature.returnClassType));
                        builder.addMethod(buildLWTNotEqual(fieldSignatureInfo, currentSignature.returnClassType));
                    });

        }
    }

    private static MethodSpec buildLWTConditionOnColumn(String relation, FieldSignatureInfo fieldSignatureInfo, TypeName currentType) {
        String methodName = "if" + upperCaseFirst(fieldSignatureInfo.fieldName) + "_" + upperCaseFirst(relation);
        return MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an ... <strong>IF $L $L ?</strong>", fieldSignatureInfo.fieldName, relationToSymbolForJavaDoc(relation))
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldSignatureInfo.typeName, fieldSignatureInfo.fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldSignatureInfo.fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldSignatureInfo.fieldName, fieldSignatureInfo.fieldName)
                .addStatement("where.onlyIf($T.$L($S, $T.bindMarker($S)))",
                        QUERY_BUILDER, relation, fieldSignatureInfo.cqlColumn, QUERY_BUILDER, fieldSignatureInfo.cqlColumn)
                .addStatement("return this")
                .returns(currentType)
                .build();

    }

    private static MethodSpec buildLWTNotEqual(FieldSignatureInfo fieldSignatureInfo, TypeName currentType) {
        String methodName = "if" + upperCaseFirst(fieldSignatureInfo.fieldName) + "_NotEq";
        return MethodSpec.methodBuilder(methodName)
                .addJavadoc("Generate an  ... <strong>IF $L != ?</strong>", fieldSignatureInfo.fieldName)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "static-access").build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldSignatureInfo.typeName, fieldSignatureInfo.fieldName, Modifier.FINAL)
                .addStatement("boundValues.add($N)", fieldSignatureInfo.fieldName)
                .addStatement("encodedValues.add(meta.$L.encodeFromJava($N))", fieldSignatureInfo.fieldName, fieldSignatureInfo.fieldName)
                .addStatement("where.onlyIf($T.of($S, $T.bindMarker($S)))",
                        NOT_EQ, fieldSignatureInfo.cqlColumn, QUERY_BUILDER, fieldSignatureInfo.cqlColumn)
                .addStatement("return this")
                .returns(currentType)
                .build();

    }

    protected static String relationToSymbolForJavaDoc(String relation) {
        switch (relation) {
            case EQ:
                return "=";
            case LT:
                return "<";
            case LTE:
                return "<=";
            case GT:
                return ">";
            case GTE:
                return ">=";
            default:
                return " ??? ";

        }
    }

    protected static String formatColumnTuplesForJavadoc(String columnTuples) {
        return "(" + columnTuples.replaceAll("\"","") + ")";
    }

    public enum ReturnType {
        NEW,
        THIS
    }

    public enum WhereClauseFor {
        STATIC,
        NORMAL
    }

    public static class FieldSignatureInfo {
        public final String fieldName;
        public final String cqlColumn;
        public final TypeName typeName;

        private FieldSignatureInfo(String fieldName, String cqlColumn, TypeName typeName) {
            this.fieldName = fieldName;
            this.cqlColumn = cqlColumn;
            this.typeName = typeName;
        }

        public static FieldSignatureInfo of(String fieldName, String cqlColumn, TypeName typeName) {
            return new FieldSignatureInfo(fieldName, cqlColumn, typeName);
        }
    }

    public static class ClassSignatureInfo {
        public final TypeName classType;
        public final TypeName returnClassType;
        public final TypeName superType;
        public final String className;


        private ClassSignatureInfo(TypeName classType, TypeName returnClassType, TypeName superType, String className) {
            this.classType = classType;
            this.returnClassType = returnClassType;
            this.superType = superType;
            this.className = className;
        }

        public static ClassSignatureInfo of(TypeName classType, TypeName returnClassType, TypeName superType, String className) {
            return new ClassSignatureInfo(classType, returnClassType, superType, className);
        }
    }

    public static class ClassSignatureParams {
        public final String dslSuffix;
        public final String whereDslSuffix;
        public final String endDslSuffix;
        public final ClassName abstractWherePartitionType;
        public final ClassName abstractWhereType;
        public final Optional<ClassName> abstractEndType;

        private ClassSignatureParams(String dslSuffix, String whereDslSuffix, String endDslSuffix, ClassName abstractWherePartitionType,
                                     ClassName abstractWhereType) {
            this.dslSuffix = dslSuffix;
            this.whereDslSuffix = whereDslSuffix;
            this.endDslSuffix = endDslSuffix;
            this.abstractWherePartitionType = abstractWherePartitionType;
            this.abstractWhereType = abstractWhereType;
            this.abstractEndType = Optional.empty();
        }

        private ClassSignatureParams(String dslSuffix, String whereDslSuffix, String endDslSuffix, ClassName abstractWherePartitionType,
                                     ClassName abstractWhereType, ClassName abstractEndType) {
            this.dslSuffix = dslSuffix;
            this.whereDslSuffix = whereDslSuffix;
            this.endDslSuffix = endDslSuffix;
            this.abstractWherePartitionType = abstractWherePartitionType;
            this.abstractWhereType = abstractWhereType;
            this.abstractEndType = Optional.of(abstractEndType);
        }

        public static ClassSignatureParams of(String dslSuffix,String whereDslSuffix, String endDslSuffix, ClassName abstractWherePartitionType,
                                              ClassName abstractWhereType) {
            return new ClassSignatureParams(dslSuffix, whereDslSuffix, endDslSuffix, abstractWherePartitionType, abstractWhereType);
        }

        public static ClassSignatureParams of(String dslSuffix, String whereDslSuffix, String endDslSuffix, ClassName abstractWherePartitionType,
                                              ClassName abstractWhereType, ClassName abstractEndType) {
            return new ClassSignatureParams(dslSuffix, whereDslSuffix, endDslSuffix, abstractWherePartitionType, abstractWhereType, abstractEndType);
        }

    }
}
