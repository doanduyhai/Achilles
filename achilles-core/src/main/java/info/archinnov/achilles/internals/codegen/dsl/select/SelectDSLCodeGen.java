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

package info.archinnov.achilles.internals.codegen.dsl.select;

import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType.NEW;
import static info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen.ReturnType.THIS;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.StringJoiner;
import javax.lang.model.element.Modifier;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.codegen.dsl.AbstractDSLCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.type.tuples.Tuple2;

public abstract class SelectDSLCodeGen extends AbstractDSLCodeGen {

    public abstract void augmentSelectClass(GlobalParsingContext context, EntityMetaSignature signature, TypeSpec.Builder builder);

    public TypeSpec buildSelectClass(GlobalParsingContext context, EntityMetaSignature signature) {

        final String firstPartitionKey = signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.PARTITION)
                .map(x -> Tuple2.of(x.context.fieldName, (PartitionKeyInfo) x.context.columnInfo))
                .sorted(TUPLE2_PARTITION_KEY_SORTER)
                .map(Tuple2::_1)
                .findFirst()
                .get();

        TypeName selectFromTypeName = ClassName.get(DSL_PACKAGE, signature.selectFromReturnType());
        TypeName selectColumnsTypeName = ClassName.get(DSL_PACKAGE, signature.selectColumnsReturnType());
        TypeName selectColumnsTypeMapTypeName = ClassName.get(DSL_PACKAGE, signature.selectColumnsTypedMapReturnType());

        SelectColumnsSignature signatureForSelectColumns = SelectColumnsSignature
                .forSelectColumns(signature.selectColumnsReturnType(),
                        signature.selectColumnsTypedMapReturnType(),
                        signature.selectFromReturnType(),
                        COLUMNS_DSL_SUFFIX);

        SelectColumnsSignature signatureForSelectColumnsTypedMap = SelectColumnsSignature
                .forSelectColumnsTypedMap(signature.selectColumnsTypedMapReturnType(),
                        signature.selectFromTypedMapReturnType(),
                        COLUMNS_TYPED_MAP_DSL_SUFFIX);

        final TypeSpec.Builder selectClassBuilder = TypeSpec.classBuilder(signature.selectClassName())
                .superclass(ABSTRACT_SELECT)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildSelectConstructor(signature))
                .addField(buildExactEntityMetaField(signature))
                .addField(buildEntityClassField(signature))
                .addType(buildSelectColumns(signature, signatureForSelectColumns))
                .addType(buildSelectColumnsTypedMap(signature, signatureForSelectColumnsTypedMap))
                .addType(buildSelectFrom(signature, firstPartitionKey).build())
                .addType(buildSelectFromTypedMap(signature, firstPartitionKey).build());

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType != ColumnType.COMPUTED && !x.isUDT())
                .forEach(x -> selectClassBuilder.addMethod(buildSelectColumnMethod(selectColumnsTypeName, x, "select", NEW)));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.isUDT())
                .forEach(x -> buildSelectUDTClassAndMethods(selectClassBuilder, selectColumnsTypeName,
                        signature.selectClassName(), "", x, "select", NEW));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .forEach(x -> selectClassBuilder.addMethod(buildSelectComputedColumnMethod(selectColumnsTypeName, x, "select", NEW)));

        selectClassBuilder.addMethod(buildSelectFunctionCallMethod(selectColumnsTypeMapTypeName, "select", NEW));

        selectClassBuilder.addMethod(buildAllColumns(selectFromTypeName, SELECT_DOT_WHERE, "select"));
        selectClassBuilder.addMethod(buildAllColumnsWithSchemaProvider(selectFromTypeName, SELECT_DOT_WHERE, "select"));

        augmentSelectClass(context, signature, selectClassBuilder);

        context.selectWhereDSLCodeGen().buildWhereClasses(context, signature).forEach(selectClassBuilder::addType);

        return selectClassBuilder.build();
    }

    public MethodSpec buildSelectConstructor(EntityMetaSignature signature) {
        String metaClassName = signature.className + META_SUFFIX;
        TypeName metaClassType = ClassName.get(ENTITY_META_PACKAGE, metaClassName);

        final MethodSpec.Builder builder = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(RUNTIME_ENGINE, "rte")
                .addParameter(metaClassType, "meta")
                .addStatement("super(rte)")
                .addStatement("this.meta = meta");

        return builder.build();
    }

    public TypeSpec buildSelectColumns(EntityMetaSignature signature, SelectColumnsSignature classesSignature) {

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
                .forEach(x -> buildSelectUDTClassAndMethods(selectColumnsBuilder, selectColumnsTypeName, signature.selectColumnsReturnType(), "", x, "selection", THIS));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .forEach(x -> selectColumnsBuilder.addMethod(buildSelectComputedColumnMethod(selectColumnsTypeName, x, "selection", THIS)));

        selectColumnsBuilder.addMethod(buildSelectFunctionCallMethod(selectColumnsTypedMapTypeName, "selection", NEW));

        selectColumnsBuilder.addMethod(buildFrom(selectFromTypeName, SELECT_DOT_WHERE, "selection"));
        selectColumnsBuilder.addMethod(buildFromWithSchemaProvider(selectFromTypeName, SELECT_DOT_WHERE, "selection"));

        return selectColumnsBuilder.build();
    }

    public TypeSpec buildSelectColumnsTypedMap(EntityMetaSignature signature, SelectColumnsSignature classesSignature) {

        TypeName selectColumnsTypedMapTypeName = ClassName.get(DSL_PACKAGE, classesSignature.selectColumnsTypedMapReturnType);

        TypeName selectFromTypedMapTypeName = ClassName.get(DSL_PACKAGE, classesSignature.selectFromTypedMapReturnType);

        final TypeSpec.Builder selectColumnsBuilder = TypeSpec.classBuilder(classesSignature.selectColumnsClassName)
                .superclass(ABSTRACT_SELECT_COLUMNS_TYPED_MAP)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(SELECT_DOT_SELECTION, "selection")
                        .addStatement("super(selection)")
                        .build());

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType != ColumnType.COMPUTED && !x.isUDT())
                .forEach(x -> selectColumnsBuilder.addMethod(buildSelectColumnMethod(selectColumnsTypedMapTypeName, x, "selection", THIS)));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.isUDT())
                .forEach(x -> buildSelectUDTClassAndMethods(selectColumnsBuilder, selectColumnsTypedMapTypeName, classesSignature.selectColumnsTypedMapReturnType, "", x, "selection", THIS));

        signature.fieldMetaSignatures
                .stream()
                .filter(x -> x.context.columnType == ColumnType.COMPUTED)
                .forEach(x -> selectColumnsBuilder.addMethod(buildSelectComputedColumnMethod(selectColumnsTypedMapTypeName, x, "selection", THIS)));

        selectColumnsBuilder.addMethod(buildSelectFunctionCallMethod(selectColumnsTypedMapTypeName, "selection", THIS));

        selectColumnsBuilder.addMethod(buildFrom(selectFromTypedMapTypeName, SELECT_DOT_WHERE, "selection"));
        selectColumnsBuilder.addMethod(buildFromWithSchemaProvider(selectFromTypedMapTypeName, SELECT_DOT_WHERE, "selection"));

        return selectColumnsBuilder.build();
    }

    public TypeSpec.Builder buildSelectFrom(EntityMetaSignature signature, String firstPartitionKey) {
        TypeName selectWhereTypeName = ClassName.get(DSL_PACKAGE, signature.selectWhereReturnType(firstPartitionKey));

        TypeName selectEndTypeName = ClassName.get(DSL_PACKAGE, signature.selectEndReturnType());

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
                        .build());
    }

    public TypeSpec.Builder buildSelectFromTypedMap(EntityMetaSignature signature, String firstPartitionKey) {
        TypeName selectWhereTypedMapTypeName = ClassName.get(DSL_PACKAGE, signature.selectWhereTypedMapReturnType(firstPartitionKey));

        TypeName selectEndTypedMapTypeName = ClassName.get(DSL_PACKAGE, signature.selectEndTypedMapReturnType());

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
                        .build());
    }

    public MethodSpec buildSelectColumnMethod(TypeName newTypeName, FieldMetaSignature parsingResult, String selectVariable, ReturnType returnType) {

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(parsingResult.context.fieldName)
                .addJavadoc("Generate a SELECT ... <strong>$L</strong> ...", parsingResult.context.quotedCqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("$L.column($S)", selectVariable, parsingResult.context.quotedCqlColumn)
                .returns(newTypeName);

        if (returnType == NEW) {
            return builder.addStatement("return new $T(select)", newTypeName).build();
        } else {
            return builder.addStatement("return this").build();
        }
    }


    public void buildSelectUDTClassAndMethods(TypeSpec.Builder parentClassBuilder, TypeName returnClassTypeName, String parentClassName,
                                               String parentQuotedCqlColumn, FieldMetaSignature fieldSignature, String selectVariable, ReturnType returnType) {
        final UDTMetaSignature udtMetaSignature = fieldSignature.udtMetaSignature.get();
        final String udtClassName = parentClassName + "." + fieldSignature.context.udtClassName();
        TypeName udtClassTypeName = ClassName.get(DSL_PACKAGE, udtClassName);
        final String quotedCqlColumn = StringUtils.isBlank(parentQuotedCqlColumn)
                ? fieldSignature.context.quotedCqlColumn
                : parentQuotedCqlColumn + "." + fieldSignature.context.quotedCqlColumn;

        final TypeSpec.Builder udtClassBuilder = TypeSpec
                .classBuilder(fieldSignature.context.udtClassName())
                .addModifiers(Modifier.PUBLIC);

        udtMetaSignature.fieldMetaSignatures
                .stream()
                .filter(x -> !x.isUDT())
                .forEach(x -> udtClassBuilder.addMethod(buildSelectUDTColumnMethod(returnClassTypeName,
                        selectVariable,
                        x.context.fieldName,
                        quotedCqlColumn + "." + x.context.quotedCqlColumn,
                        returnType)));

        udtMetaSignature.fieldMetaSignatures
                .stream()
                .filter(x -> x.isUDT())
                .forEach(x -> buildSelectUDTClassAndMethods(udtClassBuilder, returnClassTypeName, udtClassName, quotedCqlColumn, x, selectVariable, returnType));

        final MethodSpec.Builder allColumnsMethodBuilder = MethodSpec.methodBuilder("allColumns")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Generate a SELECT ... <strong>$L</strong> ...", quotedCqlColumn)
                .addStatement("$L.raw($S)", selectVariable, quotedCqlColumn)
                .returns(returnClassTypeName);

        if (returnType == NEW) {
            allColumnsMethodBuilder.addStatement("return new $T($L)", returnClassTypeName, selectVariable);
        } else {
            allColumnsMethodBuilder.addStatement("return $T.this", returnClassTypeName);
        }

        udtClassBuilder.addMethod(allColumnsMethodBuilder.build());
        parentClassBuilder.addMethod(MethodSpec.methodBuilder(fieldSignature.context.fieldName)
                .addJavadoc("Generate a SELECT ... <strong>$L(.?)</strong> ...", quotedCqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return new $T()", udtClassTypeName)
                .returns(udtClassTypeName)
                .build());
        parentClassBuilder.addType(udtClassBuilder.build());
    }

    public MethodSpec buildSelectUDTColumnMethod(TypeName newTypeName, String selectVariable,
                                                  String fieldName, String quotedCqlColumn, ReturnType returnType) {

        final MethodSpec.Builder builder = MethodSpec.methodBuilder(fieldName)
                .addJavadoc("Generate a SELECT ... <strong>$L</strong> ...", quotedCqlColumn)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("$L.raw($S)", selectVariable, quotedCqlColumn)
                .returns(newTypeName);

        if (returnType == NEW) {
            return builder.addStatement("return new $T(select)", newTypeName).build();
        } else {
            return builder.addStatement("return $T.this", newTypeName).build();
        }
    }

    public MethodSpec buildSelectFunctionCallMethod(TypeName newTypeName, String fieldName, ReturnType returnType) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("function")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .returns(newTypeName)
                .addJavadoc("Use this method to call a system or user-defined function.")
                .addJavadoc("<br/>")
                .addJavadoc("All the system functions are accessible from the <strong>{@link info.archinnov.achilles.generated.function.SystemFunctions}</strong> class")
                .addJavadoc("<br/>")
                .addJavadoc("All the user-defined functions and aggregates are accessible from the <strong>{@link info.archinnov.achilles.generated.function.FunctionsRegistry}</strong> class")
                .addJavadoc("<br/>")
                .addJavadoc("System and user-defined functions accept only appropriate type. To pass in an entity field as function argument, use ")
                .addJavadoc("the generated <strong>manager.COLUMNS</strong> class which exposes all columns with their appropriate type")
                .addJavadoc("<br/>")
                .addJavadoc("Example: ")
                .addJavadoc("<pre class=\"code\"><code class=\"java\">")
                .addJavadoc("\n")
                .addJavadoc("  {@literal @}Table\n")
                .addJavadoc("  public class MyEntity {\n")
                .addJavadoc("\n")
                .addJavadoc("      ...\n")
                .addJavadoc("\n")
                .addJavadoc("      {@literal @}Column(\"value_column\")\n")
                .addJavadoc("      private String value;\n")
                .addJavadoc("\n")
                .addJavadoc("      {@literal @}Column(\"list_of_string\")\n")
                .addJavadoc("      private List<String> strings;\n")
                .addJavadoc("\n")
                .addJavadoc("      ...\n")
                .addJavadoc("\n")
                .addJavadoc("  }\n")
                .addJavadoc("\n")
                .addJavadoc("  {@literal @}FunctionsRegistry\n")
                .addJavadoc("  public interface MyFunctions {\n")
                .addJavadoc("\n")
                .addJavadoc("       String convertListToJson(List<String> strings);\n")
                .addJavadoc("\n")
                .addJavadoc("  }\n")
                .addJavadoc("\n")
                .addJavadoc("\n")
                .addJavadoc("  ...\n")
                .addJavadoc("\n")
                .addJavadoc("\n")
                .addJavadoc("  manager\n")
                .addJavadoc("     .dsl()\n")
                .addJavadoc("     .select()\n")
                .addJavadoc("     // This call will generate SELECT cast(writetime(value_column) as text) AS writetimeOfValueAsString, ...\n")
                .addJavadoc("     .function(SystemFunctions.castAsText(SystemFunctions.writetime(manager.COLUMNS.VALUE)), \"writetimeOfValueAsString\")\n")
                .addJavadoc("     ...\n")
                .addJavadoc("\n")
                .addJavadoc("  manager\n")
                .addJavadoc("     .dsl()\n")
                .addJavadoc("     .select()\n")
                .addJavadoc("     // This call will generate SELECT convertlisttojson(list_of_string) AS strings_as_json, ...\n")
                .addJavadoc("     .function(FunctionsRegistry.convertListToJson(manager.COLUMNS.STRINGS), \"strings_as_json\")\n")
                .addJavadoc("     ...\n")
                .addJavadoc("\n")
                .addJavadoc("</code></pre>\n")
                .addJavadoc("<br/>")
                .addJavadoc("\n")
                .addJavadoc("@param functionCall the function call object\n")
                .addJavadoc("@param alias mandatory alias for this function call for easier retrieval from the ResultSet\n")
                .addJavadoc("@return a built-in function call passed to the QueryBuilder object\n")
                .addParameter(FUNCTION_CALL, "functionCall", Modifier.FINAL)
                .addParameter(STRING, "alias", Modifier.FINAL)
                .addStatement("functionCall.addToSelect($L, alias)", fieldName);

        if (returnType == NEW) {
            return builder.addStatement("return new $T(select)", newTypeName).build();
        } else {
            return builder.addStatement("return this").build();
        }
    }


    public MethodSpec buildSelectComputedColumnMethod(TypeName newTypeName, FieldMetaSignature parsingResult, String fieldName, ReturnType returnType) {

        final ComputedColumnInfo columnInfo = (ComputedColumnInfo) parsingResult.context.columnInfo;
        StringJoiner joiner = new StringJoiner(",", fieldName + ".fcall($S,", ").as($S)");
        columnInfo.functionArgs.forEach(x -> joiner.add("$L"));

        final Object[] functionName = new Object[]{columnInfo.functionName};
        final Object[] functionArgs = columnInfo
                .functionArgs
                .stream()
                .map(arg -> CodeBlock.builder().add("$T.column($S)", QUERY_BUILDER, arg).build())
                .toArray();
        final Object[] alias = new Object[]{columnInfo.alias};

        final Object[] varargs = ArrayUtils.addAll(functionName, ArrayUtils.addAll(functionArgs, alias));
        final MethodSpec.Builder builder = MethodSpec.methodBuilder(parsingResult.context.fieldName)
                .addJavadoc("Generate a SELECT ... <strong>$L($L) AS $L</strong> ...", varargs)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement(joiner.toString(), varargs)
                .returns(newTypeName);

        if (returnType == NEW) {
            return builder.addStatement("return new $T(select)", newTypeName).build();
        } else {
            return builder.addStatement("return this").build();
        }
    }


    public static class SelectColumnsSignature {
        public final String selectColumnsReturnType;
        public final String selectColumnsTypedMapReturnType;
        public final String selectFromReturnType;
        public final String selectFromTypedMapReturnType;
        public final String selectColumnsClassName;

        private SelectColumnsSignature(String selectColumnsReturnType,
                                       String selectColumnsTypedMapReturnType,
                                       String selectFromReturnType,
                                       String selectFromTypedMapReturnType,
                                       String selectColumnsClassName) {
            this.selectColumnsReturnType = selectColumnsReturnType;
            this.selectColumnsTypedMapReturnType = selectColumnsTypedMapReturnType;
            this.selectFromReturnType = selectFromReturnType;
            this.selectFromTypedMapReturnType = selectFromTypedMapReturnType;
            this.selectColumnsClassName = selectColumnsClassName;
        }

        public static SelectColumnsSignature forSelectColumns(String selectColumnsReturnType,
                                                              String selectColumnsTypedMapReturnType,
                                                              String selectFromReturnType,
                                                              String selectColumnsClassName) {

            return new SelectColumnsSignature(selectColumnsReturnType, selectColumnsTypedMapReturnType, selectFromReturnType, null, selectColumnsClassName);
        }

        public static SelectColumnsSignature forSelectColumnsTypedMap(String selectColumnsTypedMapReturnType,
                                                                      String selectFromTypedMapReturnType,
                                                                      String selectColumnsClassName) {
            return new SelectColumnsSignature(null, selectColumnsTypedMapReturnType, null, selectFromTypedMapReturnType, selectColumnsClassName);
        }
    }
}
