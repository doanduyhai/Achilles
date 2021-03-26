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

package info.archinnov.achilles.internals.codegen.index;

import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType.NEW;
import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType.THIS;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public abstract class IndexSelectDSLCodeGen extends SelectDSLCodeGen {

    public abstract void augmentSelectClass(GlobalParsingContext context, EntityMetaSignature signature, TypeSpec.Builder builder);

    public TypeSpec buildSelectClass(GlobalParsingContext context, EntityMetaSignature signature) {

        TypeName selectFromTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectFromReturnType());
        TypeName selectColumnsTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectColumnsReturnType());
        TypeName selectColumnsTypeMapTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectColumnsTypedMapReturnType());

        SelectColumnsSignature signatureForSelectColumns = SelectColumnsSignature
                .forSelectColumns(signature.indexSelectColumnsReturnType(),
                        signature.indexSelectColumnsTypedMapReturnType(),
                        signature.indexSelectFromReturnType(),
                        COLUMNS_DSL_SUFFIX);

        SelectColumnsSignature signatureForSelectColumnsTypedMap = SelectColumnsSignature
                .forSelectColumnsTypedMap(signature.indexSelectColumnsTypedMapReturnType(),
                        signature.indexSelectFromTypedMapReturnType(),
                        COLUMNS_TYPED_MAP_DSL_SUFFIX);

        final TypeSpec.Builder selectClassBuilder = TypeSpec.classBuilder(signature.indexSelectClassName())
                .superclass(ABSTRACT_SELECT)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildSelectConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addType(buildIndexedSelectColumns(signature, signatureForSelectColumns))
                .addType(buildSelectColumnsTypedMap(signature, signatureForSelectColumnsTypedMap))
                .addType(buildSelectFrom(signature))
                .addType(buildSelectFromTypedMap(signature));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType != ColumnType.COMPUTED && !x.isUDT())
                .forEach(x -> selectClassBuilder.addMethod(buildSelectColumnMethod(selectColumnsTypeName, x, "select", NEW)));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.isUDT())
                .forEach(x -> buildSelectUDTClassAndMethods(selectClassBuilder, selectColumnsTypeName,
                        signature.indexSelectClassName(), "", x, "select", NEW));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .forEach(x -> selectClassBuilder.addMethod(buildSelectComputedColumnMethod(selectColumnsTypeName, x, "select", NEW)));

        selectClassBuilder.addMethod(buildSelectFunctionCallMethod(selectColumnsTypeMapTypeName, "select", NEW));

        selectClassBuilder.addMethod(buildAllColumns(selectFromTypeName, SELECT_DOT_WHERE, "select"));
        selectClassBuilder.addMethod(buildAllColumnsWithSchemaProvider(selectFromTypeName, SELECT_DOT_WHERE, "select"));

        augmentSelectClass(context, signature, selectClassBuilder);

        context.indexSelectWhereDSLCodeGen().buildWhereClasses(context, signature).forEach(selectClassBuilder::addType);

        return selectClassBuilder.build();
    }

    public TypeSpec buildIndexedSelectColumns(EntityMetaSignature signature, SelectColumnsSignature classesSignature) {

        TypeName selectColumnsTypeName = ClassName.get(DSL_PACKAGE, classesSignature.selectColumnsReturnType);
        TypeName selectColumnsTypedMapTypeName = ClassName.get(DSL_PACKAGE, classesSignature.selectColumnsTypedMapReturnType);

        TypeName selectFromTypeName = ClassName.get(DSL_PACKAGE, classesSignature.selectFromReturnType);

        final TypeSpec.Builder selectColumnsBuilder = TypeSpec.classBuilder(classesSignature.selectColumnsClassName)
            .superclass(ABSTRACT_SELECT_COLUMNS)
            .addModifiers(Modifier.PUBLIC)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(SELECT_DOT_SELECTION, "selection")
                .addStatement("super(selection)")
                .build());

        signature.fieldMetaSignatures
            .stream()
            .filter(x -> x.context.columnType != ColumnType.COMPUTED && !x.isUDT())
            .forEach(x -> selectColumnsBuilder.addMethod(buildSelectColumnMethod(selectColumnsTypeName, x, "selection", THIS)));

        signature.fieldMetaSignatures
            .stream()
            .filter(x -> x.isUDT())
            .forEach(x -> buildSelectUDTClassAndMethods(selectColumnsBuilder, selectColumnsTypeName, signature.indexSelectColumnsReturnType(), "", x, "selection", THIS));

        signature.fieldMetaSignatures
            .stream()
            .filter(x -> x.context.columnType == ColumnType.COMPUTED)
            .forEach(x -> selectColumnsBuilder.addMethod(buildSelectComputedColumnMethod(selectColumnsTypeName, x, "selection", THIS)));

        selectColumnsBuilder.addMethod(buildSelectFunctionCallMethod(selectColumnsTypedMapTypeName, "selection", NEW));

        selectColumnsBuilder.addMethod(buildFrom(selectFromTypeName, SELECT_DOT_WHERE, "selection"));
        selectColumnsBuilder.addMethod(buildFromWithSchemaProvider(selectFromTypeName, SELECT_DOT_WHERE, "selection"));

        return selectColumnsBuilder.build();
    }

    public TypeSpec buildSelectFrom(EntityMetaSignature signature) {
        TypeName selectWhereTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectWhereReturnType());

        TypeName selectEndTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectEndReturnType());

        return TypeSpec.classBuilder(FROM_DSL_SUFFIX)
                .superclass(ABSTRACT_SELECT_FROM)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(SELECT_DOT_WHERE, "where")
                        .addParameter(OPTIONS, "cassandraOptions")
                        .addStatement("super(where, cassandraOptions)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("where")
                        .addJavadoc("Generate a SELECT ... FROM ... <strong>WHERE</strong> ...")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectWhereTypeName)
                        .returns(selectWhereTypeName)
                        .build())
                .addMethod(MethodSpec.methodBuilder("without_WHERE_Clause")
                        .addJavadoc("Generate a SELECT statement <strong>without</strong> the <strong>WHERE</strong> clause")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectEndTypeName)
                        .returns(selectEndTypeName)
                        .build())
                .build();
    }

    public TypeSpec buildSelectFromTypedMap(EntityMetaSignature signature) {
        TypeName selectWhereTypedMapTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectWhereTypedMapReturnType());

        TypeName selectEndTypedMapTypeName = ClassName.get(DSL_PACKAGE, signature.indexSelectEndTypedMapReturnType());

        return TypeSpec.classBuilder(FROM_TYPED_MAP_DSL_SUFFIX)
                .superclass(ABSTRACT_SELECT_FROM_TYPED_MAP)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(SELECT_DOT_WHERE, "where")
                        .addParameter(OPTIONS, "cassandraOptions")
                        .addStatement("super(where, cassandraOptions)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("where")
                        .addJavadoc("Generate a SELECT ... FROM ... <strong>WHERE</strong> ...")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectWhereTypedMapTypeName)
                        .returns(selectWhereTypedMapTypeName)
                        .build())
                .addMethod(MethodSpec.methodBuilder("without_WHERE_Clause")
                        .addJavadoc("Generate a SELECT statement <strong>without</strong> the <strong>WHERE</strong> clause")
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addStatement("return new $T(where, cassandraOptions)", selectEndTypedMapTypeName)
                        .returns(selectEndTypedMapTypeName)
                        .build())
                .build();
    }
}
