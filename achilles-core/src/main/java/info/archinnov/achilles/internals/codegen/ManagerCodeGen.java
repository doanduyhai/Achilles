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

package info.archinnov.achilles.internals.codegen;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class ManagerCodeGen {

    static ManagerAndDSLClasses buildManager(GlobalParsingContext context, AptUtils aptUtils, EntityMetaSignature signature) {

        final List<TypeSpec> classes = new ArrayList<>();

        final TypeSpec.Builder builder = TypeSpec.classBuilder(signature.className + MANAGER_SUFFIX)
                .superclass(genericType(ABSTRACT_MANAGER, signature.entityRawClass))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildConstructor(signature))
                .addMethod(buildCRUD(signature))
                .addMethod(buildDSL(signature))
                .addMethod(buildRawQuery(signature))
                .addField(buildExactEntityMetaField(signature));


        // CRUD
        final TypeSpec crudClass = context.crudAPICodeGen().buildCRUDClass(signature);

        // DSL
        final TypeSpec.Builder dslClass = TypeSpec.classBuilder(signature.className + DSL_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildSelectMethod(signature, SELECT_DSL_SUFFIX));
        classes.add(context.selectDSLCodeGen().buildSelectClass(context, signature));

        if (signature.isTable()) {
            dslClass.addMethod(buildDeleteMethod(signature))
                    .addMethod(buildUpdateMethod(signature));

            classes.add(context.deleteDSLCodeGen().buildDeleteClass(signature, context.deleteWhereDSLCodeGen()));
            classes.add(context.updateDSLCodeGen().buildUpdateClass(aptUtils, signature, context.updateWhereDSLCodeGen()));

            if (signature.hasStatic()) {
                classes.add(context.deleteDSLCodeGen().buildDeleteStaticClass(signature, context.deleteWhereDSLCodeGen()));
                classes.add(context.updateDSLCodeGen().buildUpdateStaticClass(aptUtils, signature, context.updateWhereDSLCodeGen()));
                dslClass.addMethod(buildDeleteStaticMethod(signature));
                dslClass.addMethod(buildUpdateStaticMethod(signature));
            }
        }


        // INDEX
        if (signature.hasIndex() && signature.isTable()) {
            builder.addMethod(buildINDEX(signature));
            final TypeSpec.Builder indexClass = TypeSpec.classBuilder(signature.className + INDEX_SUFFIX)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addMethod(buildSelectMethod(signature, INDEX_SELECT_DSL_SUFFIX));

            builder.addType(indexClass.build());

            classes.add(context.indexSelectDSLCodeGen().buildSelectClass(context, signature));
        }

        // Raw
        final TypeSpec.Builder queryClass = TypeSpec.classBuilder(signature.className + RAW_QUERY_SUFFIX)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        buildRawQueryMethods(signature).forEach(queryClass::addMethod);

        builder.addType(crudClass);
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
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("crud")
                .addJavadoc("Provide CRUD operations: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>FIND BY ID</li>\n");

        if (signature.isTable()) {
            builder.addJavadoc("   <li>INSERT</li>\n")
                    .addJavadoc("   <li>INSERT STATIC</li>\n")
                    .addJavadoc("   <li>INSERT IF NOT EXISTS</li>\n")
                    .addJavadoc("   <li>DELETE BY ID</li>\n")
                    .addJavadoc("   <li>DELETE BY ID IF NOT EXISTS</li>\n")
                    .addJavadoc("   <li>DELETE BY PARTITION</li>\n");
        }

        builder.addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", crudClass)
                .returns(crudClass);

        return builder.build();
    }

    private static MethodSpec buildDSL(EntityMetaSignature signature) {
        TypeName dslClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + DSL_SUFFIX);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("dsl")
                .addJavadoc("Provide DSL methods: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>SELECT</li>\n")
                .addJavadoc("   <li>ITERATION ON SELECT</li>\n");

        if (signature.isTable()) {
            builder.addJavadoc("   <li>UPDATE</li>\n")
                    .addJavadoc("   <li>DELETE</li>\n");
        }

        builder.addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", dslClass)
                .returns(dslClass);

        return builder.build();
    }

    private static MethodSpec buildINDEX(EntityMetaSignature signature) {
        TypeName indexDslClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + INDEX_SUFFIX);
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("indexed")
                .addJavadoc("Provide INDEX query methods: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>SELECT</li>\n")
                .addJavadoc("   <li>ITERATION ON SELECT</li>\n");
        builder.addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", indexDslClass)
                .returns(indexDslClass);

        return builder.build();
    }

    private static MethodSpec buildRawQuery(EntityMetaSignature signature) {
        TypeName dslClass = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX, signature.className + RAW_QUERY_SUFFIX);
        return MethodSpec.methodBuilder("raw")
                .addJavadoc("Provide Raw query methods: <br/>\n")
                .addJavadoc("<ul>\n")
                .addJavadoc("   <li>Typed Queries (for SELECT only)</li>\n")
                .addJavadoc("   <li>Native Queries (for any kind of statement)</li>\n")
                .addJavadoc("</ul>\n")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", dslClass)
                .returns(dslClass)
                .build();
    }

    private static MethodSpec buildSelectMethod(EntityMetaSignature signature, String suffix) {
        TypeName selectTypeName = ClassName.get(DSL_PACKAGE, signature.className + suffix);
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

    private static List<MethodSpec> buildRawQueryMethods(EntityMetaSignature signature) {
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
