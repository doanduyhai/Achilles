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

package info.archinnov.achilles.internals.codegen.dsl;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;

import java.util.*;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.FieldParser.IndexMetaSignature;
import info.archinnov.achilles.internals.utils.NamingHelper;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple4;


public abstract class AbstractDSLCodeGen {

    public static final String EQ = "eq";
    public static final String GT = "gt";
    public static final String GTE = "gte";
    public static final String LT = "lt";
    public static final String LTE = "lte";

    public static final Comparator<Tuple2<String, PartitionKeyInfo>> TUPLE2_PARTITION_KEY_SORTER =
            Comparator.comparing(o -> o._2().order);

    public static final Comparator<Tuple4<String, String, TypeName, PartitionKeyInfo>> TUPLE4_PARTITION_KEY_SORTER =
            Comparator.comparing(o -> o._4().order);

    public static final Comparator<Tuple4<String, String, TypeName, ClusteringColumnInfo>> TUPLE4_CLUSTERING_COLUMN_SORTER =
            Comparator.comparing(o -> o._4().order);

    public static final Comparator<IndexFieldSignatureInfo> INDEX_FIELD_SIGNATURE_SORTER =
            Comparator.comparing(o -> o.fieldName);

    public List<ClassSignatureInfo> buildClassesSignatureForWhereClause(EntityMetaSignature signature,
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
                    classSignatureParams.abstractWherePartitionType, className, ColumnType.PARTITION, x));
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

                signatures.add(ClassSignatureInfo.of(typeName, returnTypeName, superType, className, ColumnType.CLUSTERING, x));
            }
        }

        final String endClassName = signature.endClassName(classSignatureParams.endDslSuffix);
        final TypeName endTypeName = ClassName.get(DSL_PACKAGE, endClassName);
        final TypeName endReturnTypeName = ClassName.get(DSL_PACKAGE, signature.endReturnType(classSignatureParams.dslSuffix,
                classSignatureParams.endDslSuffix));
        final ClassName abstractEndType = classSignatureParams.abstractEndType.orElse(classSignatureParams.abstractWhereType);

        signatures.add(ClassSignatureInfo.of(endTypeName, endReturnTypeName, genericType(abstractEndType, endReturnTypeName, signature.entityRawClass),
                endClassName, null, null));

        return signatures;
    }

    public MethodSpec buildWhereConstructorWithOptions(TypeName whereType) {
        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(whereType, "where")
                .addParameter(OPTIONS, "cassandraOptions")
                .addStatement("super(where, cassandraOptions)")
                .build();

    }

    public MethodSpec buildGetThis(TypeName currentType) {
        return MethodSpec
                .methodBuilder("getThis")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .returns(currentType)
                .addStatement("return this")
                .build();
    }

    public MethodSpec buildGetMetaInternal(TypeName currentType) {

        return MethodSpec
                .methodBuilder("getMetaInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return meta")
                .returns(genericType(ABSTRACT_ENTITY_PROPERTY, currentType))
                .build();
    }

    public MethodSpec buildGetEntityClass(EntityMetaSignature signature) {
        return MethodSpec
                .methodBuilder("getEntityClass")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return entityClass")
                .returns(classTypeOf(signature.entityRawClass))
                .build();
    }

    public MethodSpec buildGetRte() {
        return MethodSpec
                .methodBuilder("getRte")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return rte")
                .returns(RUNTIME_ENGINE)
                .build();
    }

    public MethodSpec buildGetOptions() {
        return MethodSpec
                .methodBuilder("getOptions")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return cassandraOptions")
                .returns(OPTIONS)
                .build();
    }

    public MethodSpec buildGetBoundValuesInternal() {
        return MethodSpec
                .methodBuilder("getBoundValuesInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return boundValues")
                .returns(LIST_OBJECT)
                .build();
    }

    public MethodSpec buildGetEncodedBoundValuesInternal() {
        return MethodSpec
                .methodBuilder("getEncodedValuesInternal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.FINAL, Modifier.PROTECTED)
                .addStatement("return encodedValues")
                .returns(LIST_OBJECT)
                .build();
    }

    public boolean hasCounter(EntityMetaSignature signature) {
        return signature
                .fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COUNTER || x.context.columnType == ColumnType.STATIC_COUNTER)
                .count() > 0;
    }

    public FieldSpec buildExactEntityMetaField(EntityMetaSignature signature) {
        String entityMetaClassName = signature.className + META_SUFFIX;
        TypeName entityMetaExactType = ClassName.get(ENTITY_META_PACKAGE, entityMetaClassName);
        return FieldSpec.builder(entityMetaExactType, "meta", Modifier.FINAL, Modifier.PROTECTED).build();
    }

    public FieldSpec buildEntityClassField(EntityMetaSignature signature) {
        return FieldSpec.builder(classTypeOf(signature.entityRawClass), "entityClass", Modifier.FINAL, Modifier.PROTECTED)
                .initializer("$T.class", signature.entityRawClass)
                .build();
    }

    public MethodSpec buildAllColumns(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumns_FromBaseTable")
                .addJavadoc("Generate ... * FROM ...")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final $T where = $L.all().from(meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName()), meta.getTableOrViewName()).where()",
                        whereTypeName, privateFieldName, "unknown_keyspace_for_")
                .addStatement("return new $T(where, new $T())", newTypeName, OPTIONS)
                .returns(newTypeName)
                .build();
    }

    public MethodSpec buildAllColumnsWithSchemaProvider(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("allColumns_From")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate ... * FROM ... using the given SchemaNameProvider")
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $L.all().from(currentKeyspace, currentTable).where()", whereTypeName, privateFieldName)
                .addStatement("return new $T(where, $T.withSchemaNameProvider(schemaNameProvider))", newTypeName, OPTIONS)
                .returns(newTypeName)
                .build();
    }

    public MethodSpec buildFrom(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("fromBaseTable")
                .addJavadoc("Generate a ... <strong>FROM xxx</strong> ... ")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("final $T where = $L.from(meta.getKeyspace().orElse($S + meta.entityClass.getCanonicalName()), " +
                        "meta.getTableOrViewName()).where()", whereTypeName, privateFieldName, "unknown_keyspace_for_")
                .addStatement("return new $T(where, new $T())", newTypeName, OPTIONS)
                .returns(newTypeName)
                .build();
    }

    public MethodSpec buildFromWithSchemaProvider(TypeName newTypeName, TypeName whereTypeName, String privateFieldName) {
        return MethodSpec.methodBuilder("from")
                .addJavadoc("Generate a ... <strong>FROM xxx</strong> ... using the given SchemaNameProvider")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("final String currentKeyspace = lookupKeyspace(schemaNameProvider, meta.entityClass)")
                .addStatement("final String currentTable = lookupTable(schemaNameProvider, meta.entityClass)")
                .addStatement("final $T where = $L.from(currentKeyspace, currentTable).where()", whereTypeName, privateFieldName)
                .addStatement("return new $T(where, $T.withSchemaNameProvider(schemaNameProvider))", newTypeName, OPTIONS)
                .returns(newTypeName)
                .build();
    }

    public static MethodSpec buildRelationMethod(String fieldName, TypeName relationClassTypeName) {
        return MethodSpec.methodBuilder(fieldName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", relationClassTypeName)
                .returns(relationClassTypeName)
                .build();
    }

    public List<FieldSignatureInfo> getPartitionKeysSignatureInfo(List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple4.of(x.context.fieldName, x.context.cqlColumn, x.sourceType, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(TUPLE4_PARTITION_KEY_SORTER)
                .map(x -> FieldSignatureInfo.of(x._1(), x._2(), x._3()))
                .collect(toList()));
    }

    public List<FieldSignatureInfo> getClusteringColsSignatureInfo(List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.columnType == ColumnType.CLUSTERING)
                .map(x -> Tuple4.of(x.context.fieldName, x.context.cqlColumn, x.sourceType, (ClusteringColumnInfo) x.context.columnInfo))
                .sorted(TUPLE4_CLUSTERING_COLUMN_SORTER)
                .map(x -> FieldSignatureInfo.of(x._1(), x._2(), x._3()))
                .collect(toList()));
    }

    public List<IndexFieldSignatureInfo> getIndexedColsSignatureInfo(IndexImpl indexImpl, List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.indexInfo.type != IndexType.NONE)
                .filter(x -> x.context.indexInfo.impl == indexImpl)
                .map(x -> IndexFieldSignatureInfo.of(x.context.fieldName, x.context.cqlColumn, x.sourceType, x.context.indexInfo, x.indexMetaSignature))
                .sorted(INDEX_FIELD_SIGNATURE_SORTER)
                .collect(toList()));
    }

    public static List<IndexFieldSignatureInfo> getDSESearchColsSignatureInfo(List<FieldMetaSignature> parsingResults) {
        return new ArrayList<>(parsingResults
                .stream()
                .filter(x -> x.context.indexInfo.type != IndexType.NONE)
                .filter(x -> x.context.indexInfo.impl == IndexImpl.DSE_SEARCH)
                .map(x -> IndexFieldSignatureInfo.of(x.context.fieldName, x.context.cqlColumn, x.targetType, x.context.indexInfo, x.indexMetaSignature))
                .sorted(INDEX_FIELD_SIGNATURE_SORTER)
                .collect(toList()));
    }

    public static String relationToSymbolForJavaDoc(String relation) {
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

    public static String formatColumnTuplesForJavadoc(String columnTuples) {
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
        public final String quotedCqlColumn;
        public final TypeName typeName;

        private FieldSignatureInfo(String fieldName, String cqlColumn, TypeName typeName) {
            this.fieldName = fieldName;
            this.cqlColumn = cqlColumn;
            this.quotedCqlColumn = NamingHelper.maybeQuote(cqlColumn);
            this.typeName = typeName;
        }

        public static FieldSignatureInfo of(String fieldName, String cqlColumn, TypeName typeName) {
            return new FieldSignatureInfo(fieldName, cqlColumn, typeName);
        }
    }

    public static class IndexFieldSignatureInfo extends FieldSignatureInfo {
        public final IndexInfo indexInfo;
        public final IndexMetaSignature indexMetaSignature;

        private IndexFieldSignatureInfo(String fieldName, String cqlColumn, TypeName typeName, IndexInfo indexInfo,
                                        IndexMetaSignature indexMetaSignature) {
            super(fieldName, cqlColumn, typeName);
            this.indexInfo = indexInfo;
            this.indexMetaSignature = indexMetaSignature;
        }

        public static IndexFieldSignatureInfo of(String fieldName, String cqlColumn, TypeName typeName, IndexInfo indexInfo,
                                                 IndexMetaSignature indexMetaSignature) {
            return new IndexFieldSignatureInfo(fieldName, cqlColumn, typeName, indexInfo, indexMetaSignature);
        }
    }

    public static class ClassSignatureInfo {
        public final TypeName classType;
        public final TypeName returnClassType;
        public final TypeName superType;
        public final String className;
        public final ColumnType columnType;
        public final FieldSignatureInfo fieldSignatureInfo;


        private ClassSignatureInfo(TypeName classType, TypeName returnClassType, TypeName superType, String className, ColumnType columnType, FieldSignatureInfo fieldSignatureInfo) {
            this.classType = classType;
            this.returnClassType = returnClassType;
            this.superType = superType;
            this.className = className;
            this.columnType = columnType;
            this.fieldSignatureInfo = fieldSignatureInfo;
        }

        public static ClassSignatureInfo of(TypeName classType, TypeName returnClassType, TypeName superType, String className, ColumnType columnType, FieldSignatureInfo fieldSignatureInfo) {
            return new ClassSignatureInfo(classType, returnClassType, superType, className, columnType, fieldSignatureInfo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassSignatureInfo that = (ClassSignatureInfo) o;
            return Objects.equals(classType, that.classType) &&
                    Objects.equals(returnClassType, that.returnClassType) &&
                    Objects.equals(superType, that.superType) &&
                    Objects.equals(className, that.className) &&
                    columnType == that.columnType &&
                    Objects.equals(fieldSignatureInfo, that.fieldSignatureInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classType, returnClassType, superType, className, columnType, fieldSignatureInfo);
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
