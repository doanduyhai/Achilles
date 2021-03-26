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

package info.archinnov.achilles.internals.codegen.crud;

import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.CLUSTERING;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.PARTITION;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.Comparator;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.type.tuples.Tuple3;

public abstract class CrudAPICodeGen {

    public static final Comparator<Tuple3<String, TypeName, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            (o1, o2) -> o1._3().order.compareTo(o2._3().order);
    public static final Comparator<Tuple3<String, TypeName, ClusteringColumnInfo>> CLUSTERING_COLUMN_SORTER =
            (o1, o2) -> o1._3().order.compareTo(o2._3().order);

    protected abstract void augmentCRUDClass(EntityMetaSignature signature, TypeSpec.Builder crudClassBuilder);

    public TypeSpec buildCRUDClass(EntityMetaSignature signature) {

        final TypeSpec.Builder crudClass = TypeSpec.classBuilder(signature.className + CRUD_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addField(FieldSpec.builder(genericType(OPTIONAL, OPTIONS), "cassandraOptions", Modifier.PRIVATE)
                        .initializer(CodeBlock.builder().addStatement("$T.empty()", OPTIONAL).build()).build())
                .addMethod(buildWithSchemaNameProvider(signature))
                .addMethod(buildFind(signature));

        // API for table
        if (signature.isTable()) {
            crudClass.addMethod(buildDeleteInstance(signature))
                    .addMethod(buildDeleteByKeys(signature));

            if (!signature.isCounterEntity()) {
                crudClass.addMethod(buildInsert(signature));
                crudClass.addMethod(buildUpdate(signature));
                if (signature.hasStatic()) {
                    crudClass.addMethod(buildInsertStatic(signature));
                    crudClass.addMethod(buildUpdateStatic(signature));
                }
            }

            if (signature.hasClustering()) {
                crudClass.addMethod(buildDeleteByPartition(signature));
            }
        }

        augmentCRUDClass(signature, crudClass);

        return crudClass.build();
    }

    private static MethodSpec buildWithSchemaNameProvider(EntityMetaSignature signature) {
        TypeName crudClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX + "." + signature.className + CRUD_SUFFIX);
        return MethodSpec.methodBuilder("withSchemaNameProvider")
                .addModifiers(Modifier.PUBLIC)
                .addParameter(SCHEMA_NAME_PROVIDER, "schemaNameProvider", Modifier.FINAL)
                .addStatement("$T.validateNotNull($N,$S)", VALIDATOR, "schemaNameProvider", "The provided schemaNameProvider should not be null")
                .addStatement("this.cassandraOptions = $T.of($T.withSchemaNameProvider($N))", OPTIONAL, OPTIONS, "schemaNameProvider")
                .addStatement("return this")
                .returns(crudClass)
                .build();
    }

    private static MethodSpec buildFind(EntityMetaSignature signature) {
        ParameterizedTypeName returnType = genericType(FIND_WITH_OPTIONS, signature.entityRawClass);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("findById")
                .addJavadoc("Find an entity by its complete primary key")
                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T keys = new $T<>()", LIST_OBJECT, ARRAY_LIST)
                .addStatement("$T encodedKeys = new $T<>()", LIST_OBJECT, ARRAY_LIST);

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> Tuple3.of(x.context.fieldName, x.sourceType, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .forEach(tuple ->
                        builder.addJavadoc("@param $L partition key '$L'", tuple._1(), tuple._1())
                                .addParameter(tuple._2(), tuple._1(), Modifier.FINAL)
                                .addStatement("$T.validateNotNull($L, $S, $S)", VALIDATOR, tuple._1(),
                                        "Partition key '%s' should not be null", tuple._1())
                                .addStatement("keys.add($L)", tuple._1())
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N, cassandraOptions))",
                                        signature.className + META_SUFFIX, tuple._1(), tuple._1())
                );

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple3.of(x.context.fieldName, x.sourceType, (ClusteringColumnInfo) x.context.columnInfo))
                .sorted(CLUSTERING_COLUMN_SORTER)
                .forEach(tuple ->
                        builder.addJavadoc("@param $L clustering column '$L'", tuple._1(), tuple._1())
                                .addParameter(tuple._2(), tuple._1(), Modifier.FINAL)
                                .addStatement("$T.validateNotNull($L, $S, $S)", VALIDATOR, tuple._1(),
                                        "Partition key '%s' should not be null", tuple._1())
                                .addStatement("keys.add($L)", tuple._1())
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N, cassandraOptions))",
                                        signature.className + META_SUFFIX, tuple._1(), tuple._1())
                );


        builder.addJavadoc("@return FindWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] primaryKeyValues = keys.toArray()")
                .addStatement("final Object[] encodedPrimaryKeyValues = encodedKeys.toArray()")
                .addStatement("return new $T(entityClass, meta, rte, primaryKeyValues, encodedPrimaryKeyValues, cassandraOptions)", returnType)
                .returns(returnType);

        return builder.build();
    }

    private static MethodSpec buildInsert(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("insert")
                .addJavadoc("Insert this entity\n\n")
                .addJavadoc("@param instance an instance of $T\n", signature.entityRawClass)
                .addJavadoc("@return $T<$T>", INSERT_WITH_OPTIONS, signature.entityRawClass)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return insertInternal(instance, false, cassandraOptions)") // insertStatic = false
                .returns(genericType(INSERT_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }

    private static MethodSpec buildUpdate(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("update")
                .addJavadoc("Update the cassandra table with <strong>NOT NULL</strong> fields extracted from this entity\n\n")
                .addJavadoc("@param instance an instance of $T\n", signature.entityRawClass)
                .addJavadoc("@return $T<$T>", INSERT_WITH_OPTIONS, signature.entityRawClass)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return updateInternal(instance, false, cassandraOptions)") // insertStatic = false
                .returns(genericType(UPDATE_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }


    private static MethodSpec buildInsertStatic(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("insertStatic")
                .addJavadoc("Insert only partition key(s) and static column(s).\n\n")
                .addJavadoc("<strong>All clustering column(s) values will be ignored and not inserted</strong>\n\n")
                .addJavadoc("@param instance an instance of $T\n", signature.entityRawClass)
                .addJavadoc("@return $T<$T>", INSERT_WITH_OPTIONS, signature.entityRawClass)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return insertInternal(instance, true, cassandraOptions)") // insertStatic = true
                .returns(genericType(INSERT_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }

    private static MethodSpec buildUpdateStatic(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("updateStatic")
                .addJavadoc("Update only static columns of the cassandra table with <strong>NOT NULL</strong> fields extracted from this entity\n\n")
                .addJavadoc("<strong>All non-static column(s) values will be ignored and not updated</strong>\n\n")
                .addJavadoc("@param instance an instance of $T\n", signature.entityRawClass)
                .addJavadoc("@return $T<$T>", INSERT_WITH_OPTIONS, signature.entityRawClass)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return updateInternal(instance, true, cassandraOptions)") // insertStatic = true
                .returns(genericType(UPDATE_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }

    /*
       public DeleteWithOptions delete(...) {
         validate keys not null
         return DeleteWithOptions(rte, clazz, meta, keys);
       }
    */
    private static MethodSpec buildDeleteByKeys(EntityMetaSignature signature) {
        ParameterizedTypeName returnType = genericType(DELETE_WITH_OPTIONS, signature.entityRawClass);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("deleteById")
                .addJavadoc("Delete an entity using its complete primary key")
                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T keys = new $T<>()", LIST_OBJECT, ARRAY_LIST)
                .addStatement("$T encodedKeys = new $T<>()", LIST_OBJECT, ARRAY_LIST);

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> Tuple3.of(x.context.fieldName, x.sourceType, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .forEach(tuple ->
                        builder.addJavadoc("@param $L partition key '$L'", tuple._1(), tuple._1())
                                .addParameter(tuple._2(), tuple._1(), Modifier.FINAL)
                                .addStatement("$T.validateNotNull($L, $S, $S)", VALIDATOR, tuple._1(),
                                        "Partition key '%s' should not be null", tuple._1())
                                .addStatement("keys.add($L)", tuple._1())
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N, cassandraOptions))",
                                        signature.className + META_SUFFIX, tuple._1(), tuple._1())
                );

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == CLUSTERING)
                .map(x -> Tuple3.of(x.context.fieldName, x.sourceType, (ClusteringColumnInfo) x.context.columnInfo))
                .sorted(CLUSTERING_COLUMN_SORTER)
                .forEach(tuple ->
                        builder.addJavadoc("@param $L clustering column '$L'", tuple._1(), tuple._1())
                                .addParameter(tuple._2(), tuple._1(), Modifier.FINAL)
                                .addStatement("$T.validateNotNull($L, $S, $S)", VALIDATOR, tuple._1(),
                                        "Partition key '%s' should not be null", tuple._1())
                                .addStatement("keys.add($L)", tuple._1())
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N, cassandraOptions))",
                                        signature.className + META_SUFFIX, tuple._1(), tuple._1()));

        builder.addJavadoc("@return DeleteWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] partitionKeysValues = keys.toArray()")
                .addStatement("final Object[] encodedPartitionKeyValues = encodedKeys.toArray()")
                .addStatement("return new $T(entityClass, meta, rte, partitionKeysValues, encodedPartitionKeyValues, $T.empty(), cassandraOptions)", returnType, OPTIONAL)
                .returns(returnType);


        return builder.build();
    }

    private static MethodSpec buildDeleteInstance(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("delete")
                .addJavadoc("Delete an entity instance by extracting its primary key")
                .addJavadoc("Remark: <strong>Achilles will throw an exception if any column being part of the primary key is NULL</strong>")
                .addJavadoc("@param an instance of $T to be delete", signature.entityRawClass)
                .addJavadoc("@return DeleteWithOptions<$T>", signature.entityRawClass)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return deleteInternal($N, cassandraOptions)", "instance")
                .returns(genericType(DELETE_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }

    private static MethodSpec buildDeleteByPartition(EntityMetaSignature signature) {
        ParameterizedTypeName returnType = genericType(DELETE_BY_PARTITION_WITH_OPTIONS, signature.entityRawClass);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("deleteByPartitionKeys")
                .addJavadoc("Delete a whole partition using the partition key")

                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T keys = new $T<>()", LIST_OBJECT, ARRAY_LIST)
                .addStatement("$T encodedKeys = new $T<>()", LIST_OBJECT, ARRAY_LIST);

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == PARTITION)
                .map(x -> Tuple3.of(x.context.fieldName, x.sourceType, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(PARTITION_KEY_SORTER)
                .forEach(tuple ->
                        builder.addJavadoc("@param $L partition key '$L'", tuple._1(), tuple._1())
                                .addParameter(tuple._2(), tuple._1(), Modifier.FINAL)
                                .addStatement("$T.validateNotNull($L, $S, $S)", VALIDATOR, tuple._1(),
                                        "Partition key '%s' should not be null", tuple._1())
                                .addStatement("keys.add($L)", tuple._1())
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N, cassandraOptions))",
                                        signature.className + META_SUFFIX, tuple._1(), tuple._1()));


        builder.addJavadoc("@return DeleteByPartitionWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] partitionKeys = keys.toArray()")
                .addStatement("final Object[] encodedPartitionKeys = encodedKeys.toArray()")
                .addStatement("return new $T(meta, rte, partitionKeys, encodedPartitionKeys, cassandraOptions)", returnType)
                .returns(returnType);

        return builder.build();
    }
}
