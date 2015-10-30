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

package info.archinnov.achilles.internals.codegen;

import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.CLUSTERING;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.PARTITION;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.type.tuples.Tuple3;

public class ManagerCodeGen {

    public static final Comparator<Tuple3<String, TypeName, PartitionKeyInfo>> PARTITION_KEY_SORTER =
            (o1, o2) -> o1._3().order.compareTo(o2._3().order);
    public static final Comparator<Tuple3<String, TypeName, ClusteringColumnInfo>> CLUSTERING_COLUMN_SORTER =
            (o1, o2) -> o1._3().order.compareTo(o2._3().order);

    static ManagerAndDSLClasses buildManager(AptUtils aptUtils, EntityMetaSignature signature) {

        final List<TypeSpec> classes = new ArrayList<>();

        final TypeSpec.Builder builder = TypeSpec.classBuilder(signature.className + MANAGER_SUFFIX)
                .superclass(genericType(ABSTRACT_MANAGER, signature.entityRawClass))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildConstructor(signature))
                .addMethod(buildCRUD(signature))
                .addMethod(buildDSL(signature))
                .addMethod(buildQuery(signature))
                .addField(buildExactEntityMetaField(signature));


        // CRUD
        final TypeSpec.Builder crudClass = TypeSpec.classBuilder(signature.className + CRUD_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildFind(signature))
                .addMethod(buildDeleteInstance(signature))
                .addMethod(buildDeleteByKeys(signature));

        if (signature.hasClustering()) {
            crudClass.addMethod(buildDeleteByPartition(signature));
        }

        if (!signature.isCounterEntity()) {
            crudClass.addMethod(buildInsert(signature));
        }

        // DSL
        final TypeSpec.Builder dslClass = TypeSpec.classBuilder(signature.className + DSL_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildSelectMethod(signature))
                .addMethod(buildDeleteMethod(signature))
                .addMethod(buildUpdateMethod(signature));


        classes.add(SelectDSLCodeGen.buildSelectClass(signature));
        classes.add(DeleteDSLCodeGen.buildDeleteClass(signature));
        classes.add(UpdateDSLCodeGen.buildUpdateClass(aptUtils, signature));

        if (signature.hasStatic()) {
            classes.add(DeleteDSLCodeGen.buildDeleteStaticClass(signature));
            classes.add(UpdateDSLCodeGen.buildUpdateStaticClass(aptUtils, signature));
            dslClass.addMethod(buildDeleteStaticMethod(signature));
            dslClass.addMethod(buildUpdateStaticMethod(signature));

        }

        // Query
        final TypeSpec.Builder queryClass = TypeSpec.classBuilder(signature.className + QUERY_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        buildQueryMethods(signature).forEach(queryClass::addMethod);

        builder.addType(crudClass.build());
        builder.addType(dslClass.build());
        builder.addType(queryClass.build());

        return new ManagerAndDSLClasses(builder.build(), classes);
    }

    private static MethodSpec buildConstructor(EntityMetaSignature signature) {
        String entityMetaClassName = signature.className + META_SUFFIX;
        TypeName entityMetaExactType = ClassName.get(ENTITY_META_PACKAGE, entityMetaClassName);
        return MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(TypeUtils.classTypeOf(signature.entityRawClass), "entityClass", Modifier.FINAL)
                .addParameter(entityMetaExactType, "meta", Modifier.FINAL)
                .addParameter(RUNTIME_ENGINE, "rte", Modifier.FINAL)
                .addStatement("super($N, $N, $N)", "entityClass", "meta", "rte")
                .addStatement("this.meta = meta")
                .build();
    }

    private static MethodSpec buildCRUD(EntityMetaSignature signature) {
        TypeName crudClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + CRUD_SUFFIX);
        return MethodSpec.methodBuilder("crud")
                .addJavadoc("Provide CRUD operations: <br/> \n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>INSERT</li>\n")
                .addJavadoc("   <li>INSERT IF NOT EXISTS</li>\n")
                .addJavadoc("   <li>FIND BY ID</li>\n")
                .addJavadoc("   <li>DELETE BY ID</li>\n")
                .addJavadoc("   <li>DELETE BY ID IF NOT EXISTS</li>\n")
                .addJavadoc("   <li>DELETE BY PARTITION</li>\n")
                .addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", crudClass)
                .returns(crudClass)
                .build();
    }

    private static MethodSpec buildDSL(EntityMetaSignature signature) {
        TypeName dslClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + DSL_SUFFIX);
        return MethodSpec.methodBuilder("dsl")
                .addJavadoc("Provide DSL methods: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>SELECT</li>\n")
                .addJavadoc("   <li>ITERATION ON SELECT</li>\n")
                .addJavadoc("   <li>UPDATE</li>\n")
                .addJavadoc("   <li>DELETE</li>\n")
                .addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", dslClass)
                .returns(dslClass)
                .build();
    }

    private static MethodSpec buildQuery(EntityMetaSignature signature) {
        TypeName dslClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + QUERY_SUFFIX);
        return MethodSpec.methodBuilder("query")
                .addJavadoc("Provide QUERY methods: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>Typed Queries (for SELECT only)</li>\n")
                .addJavadoc("   <li>Native Queries (for any kind of statement)</li>\n")
                .addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", dslClass)
                .returns(dslClass)
                .build();
    }

    private static MethodSpec buildInsert(EntityMetaSignature signature) {
        return MethodSpec.methodBuilder("insert")
                .addJavadoc("Insert this entity")
                .addJavadoc("@param instance an instance of $T", signature.entityRawClass)
                .addJavadoc("@return InsertWithOptions<$T>", signature.entityRawClass)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addParameter(signature.entityRawClass, "instance", Modifier.FINAL)
                .addStatement("return insertInternal(instance)")
                .returns(genericType(INSERT_WITH_OPTIONS, signature.entityRawClass))
                .build();
    }

    private static MethodSpec buildFind(EntityMetaSignature signature) {
        ParameterizedTypeName returnType = genericType(FIND_WITH_OPTIONS, signature.entityRawClass);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("findById")
                .addJavadoc("Find an entity by its complete primary key")
                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T keys = new $T<>()", LIST_OBJECT, ARRAY_LIST)
                .addStatement("$T encodedKeys = new $T<>()", LIST_OBJECT, ARRAY_LIST);

        signature.parsingResults
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
                                        .addStatement("encodedKeys.add($L.$L.encodeFromJava($N))", signature.className + META_SUFFIX,
                                                tuple._1(), tuple._1(), tuple._1())
                );

        signature.parsingResults
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
                                        .addStatement("encodedKeys.add($L.$L.encodeFromJava($N))", signature.className + META_SUFFIX,
                                                tuple._1(), tuple._1(), tuple._1())
                );


        builder.addJavadoc("@return FindWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] primaryKeyValues = keys.toArray()")
                .addStatement("final Object[] encodedPrimaryKeyValues = encodedKeys.toArray()")
                .addStatement("return new $T($L, $L, $L, $L, $L)", returnType,
                        "entityClass", "meta", "rte", "primaryKeyValues", "encodedPrimaryKeyValues")
                .returns(returnType);

        return builder.build();
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

        signature.parsingResults
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
                                        .addStatement("encodedKeys.add($L.$L.encodeFromJava($N))", signature.className + META_SUFFIX,
                                                tuple._1(), tuple._1(), tuple._1())
                );

        signature.parsingResults
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
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N))", signature.className + META_SUFFIX,
                                        tuple._1(), tuple._1(), tuple._1()));

        builder.addJavadoc("@return DeleteWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] partitionKeysValues = keys.toArray()")
                .addStatement("final Object[] encodedPartitionKeyValues = encodedKeys.toArray()")
                .addStatement("return new $T($L, $L, $L, $L, $L, $T.empty())", returnType,
                        "entityClass", "meta", "rte", "partitionKeysValues", "encodedPartitionKeyValues", OPTIONAL)
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
                .addStatement("return deleteInternal($N)", "instance")
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

        signature.parsingResults
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
                                .addStatement("encodedKeys.add($L.$L.encodeFromJava($N))", signature.className + META_SUFFIX,
                                        tuple._1(), tuple._1(), tuple._1()));


        builder.addJavadoc("@return DeleteByPartitionWithOptions<$T>", signature.entityRawClass);

        builder.addStatement("final Object[] partitionKeys = keys.toArray()")
                .addStatement("final Object[] encodedPartitionKeys = encodedKeys.toArray()")
                .addStatement("return new $T($L, $L, $L, $L)", returnType,
                        "meta", "rte", "partitionKeys", "encodedPartitionKeys")
                .returns(returnType);

        return builder.build();
    }

    private static MethodSpec buildSelectMethod(EntityMetaSignature signature) {
        TypeName selectTypeName = ClassName.get(DSL_PACKAGE, signature.className + SELECT_DSL_SUFFIX);
        return MethodSpec.methodBuilder("select")
                .addJavadoc("Generate a <strong>SELECT</strong> statement")
                .addJavadoc("@return $T", selectTypeName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(rte, meta)", selectTypeName)
                .returns(selectTypeName)
                .build();
    }

    private static MethodSpec buildDeleteMethod(EntityMetaSignature signature) {
        TypeName deleteTypeName = ClassName.get(DSL_PACKAGE, signature.className + DELETE_DSL_SUFFIX);
        return MethodSpec.methodBuilder("delete")
                .addJavadoc("Generate a <strong>DELETE</strong> statement")
                .addJavadoc("@return $T", deleteTypeName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(rte, meta)", deleteTypeName)
                .returns(deleteTypeName)
                .build();
    }

    private static MethodSpec buildDeleteStaticMethod(EntityMetaSignature signature) {
        TypeName deleteStaticTypeName = ClassName.get(DSL_PACKAGE, signature.className + DELETE_STATIC_DSL_SUFFIX);
        return MethodSpec.methodBuilder("deleteStatic")
                .addJavadoc("Generate a <strong>DELETE</strong> statement for <strong>static</strong> columns only")
                .addJavadoc("(requiring only partition key(s))")
                .addJavadoc("@return $T", deleteStaticTypeName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(rte, meta)", deleteStaticTypeName)
                .returns(deleteStaticTypeName)
                .build();
    }

    private static MethodSpec buildUpdateMethod(EntityMetaSignature signature) {
        TypeName updateTypeName = ClassName.get(DSL_PACKAGE, signature.className + UPDATE_DSL_SUFFIX);
        return MethodSpec.methodBuilder("update")
                .addJavadoc("Generate an <strong>UPDATE</strong> statement")
                .addJavadoc("@return $T", updateTypeName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(rte, meta)", updateTypeName)
                .returns(updateTypeName)
                .build();
    }

    private static MethodSpec buildUpdateStaticMethod(EntityMetaSignature signature) {
        TypeName updateStaticTypeName = ClassName.get(DSL_PACKAGE, signature.className + UPDATE_STATIC_DSL_SUFFIX);
        return MethodSpec.methodBuilder("updateStatic")
                .addJavadoc("Generate an <strong>UPDATE</strong> statement for <strong>static</strong> columns only")
                .addJavadoc("(requiring only partition key(s))")
                .addJavadoc("@return $T", updateStaticTypeName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T(rte, meta)", updateStaticTypeName)
                .returns(updateStaticTypeName)
                .build();
    }

    private static List<MethodSpec> buildQueryMethods(EntityMetaSignature signature) {
        List<MethodSpec> methods = new ArrayList<>();

        methods.add(MethodSpec.methodBuilder("typedQueryForSelect")
                .addJavadoc("Execute the bound statement and map the result back into an instance of $T <br/>\n", signature.entityRawClass)
                .addJavadoc("Remark: the bound statement should be a <strong>SELECT</strong> statement")
                .addJavadoc("@param boundStatement a bound statement")
                .addJavadoc("@return $T", genericType(TYPED_QUERY, signature.entityRawClass))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(BOUND_STATEMENT, "boundStatement")
                .addStatement("return typedQueryForSelectInternal($N)", "boundStatement")
                .returns(genericType(TYPED_QUERY, signature.entityRawClass))
                .build());

        methods.add(MethodSpec.methodBuilder("typedQueryForSelect")
                .addJavadoc("Execute the prepared statement and map the result back into an instance of $T <br/>\n", signature.entityRawClass)
                .addJavadoc("Remark: the prepared statement should be a <strong>SELECT</strong> statement")
                .addJavadoc("@param preparedStatement a prepared statement")
                .addJavadoc("@return $T", genericType(TYPED_QUERY, signature.entityRawClass))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(PREPARED_STATEMENT, "preparedStatement")
                .addParameter(ArrayTypeName.of(TypeName.OBJECT), "encodedBoundValues")
                .varargs()
                .addStatement("return typedQueryForSelectInternal($N, $N)", "preparedStatement", "encodedBoundValues")
                .returns(genericType(TYPED_QUERY, signature.entityRawClass))
                .build());

        methods.add(MethodSpec.methodBuilder("typedQueryForSelect")
                .addJavadoc("Execute the regular statement and map the result back into an instance of $T <br/>\n", signature.entityRawClass)
                .addJavadoc("Remark: the regular statement should be a <strong>SELECT</strong> statement")
                .addJavadoc("@param regularStatement a regular statement")
                .addJavadoc("@return $T", genericType(TYPED_QUERY, signature.entityRawClass))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(REGULAR_STATEMENT, "regularStatement")
                .addParameter(ArrayTypeName.of(TypeName.OBJECT), "encodedBoundValues")
                .varargs()
                .addStatement("return typedQueryForSelectInternal($N, $N)", "regularStatement", "encodedBoundValues")
                .returns(genericType(TYPED_QUERY, signature.entityRawClass))
                .build());

        methods.add(MethodSpec.methodBuilder("nativeQuery")
                .addJavadoc("Execute the native bound statement")
                .addJavadoc("@param boundStatement a bound statement")
                .addJavadoc("@return $T", NATIVE_QUERY)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(BOUND_STATEMENT, "boundStatement")
                .addStatement("return nativeQueryInternal($N)", "boundStatement")
                .returns(NATIVE_QUERY)
                .build());

        methods.add(MethodSpec.methodBuilder("nativeQuery")
                .addJavadoc("Execute the native prepared statement")
                .addJavadoc("@param preparedStatement a prepared statement")
                .addJavadoc("@return $T", NATIVE_QUERY)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(PREPARED_STATEMENT, "preparedStatement")
                .addParameter(ArrayTypeName.of(TypeName.OBJECT), "encodedBoundValues")
                .varargs()
                .addStatement("return nativeQueryInternal($N, $N)", "preparedStatement", "encodedBoundValues")
                .returns(NATIVE_QUERY)
                .build());

        methods.add(MethodSpec.methodBuilder("nativeQuery")
                .addJavadoc("Execute the native regular statement")
                .addJavadoc("@param regularStatement a regular statement")
                .addJavadoc("@return $T", NATIVE_QUERY)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(REGULAR_STATEMENT, "regularStatement")
                .addParameter(ArrayTypeName.of(TypeName.OBJECT), "encodedBoundValues")
                .varargs()
                .addStatement("return nativeQueryInternal($N, $N)", "regularStatement", "encodedBoundValues")
                .returns(NATIVE_QUERY)
                .build());

        return methods;
    }

    private static FieldSpec buildExactEntityMetaField(EntityMetaSignature signature) {
        String entityMetaClassName = signature.className + META_SUFFIX;
        TypeName entityMetaExactType = ClassName.get(ENTITY_META_PACKAGE, entityMetaClassName);
        return FieldSpec.builder(entityMetaExactType, "meta", Modifier.FINAL, Modifier.PUBLIC).build();
    }

    public static class ManagerAndDSLClasses {
        public final TypeSpec managerClass;
        public final List<TypeSpec> dslClasses;

        public ManagerAndDSLClasses(TypeSpec managerClass, List<TypeSpec> dslClasses) {
            this.managerClass = managerClass;
            this.dslClasses = dslClasses;
        }
    }
}
