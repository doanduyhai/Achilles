/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.strategy.naming.SnakeCaseNaming;

public class EntityMetaColumnsForFunctionsCodeGen  {

    private static final SnakeCaseNaming SNAKE_CASE_NAMING = new SnakeCaseNaming();

    public static final TypeSpec createColumnsClassForFunctionParam(List<FieldMetaSignature> parsingResults) {
        final TypeSpec.Builder builder = TypeSpec.classBuilder(COLUMNS_FOR_FUNCTIONS_CLASS)
                .addJavadoc("Utility class to expose all fields with their CQL type for function call")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        parsingResults
                .stream()
                .filter(x -> x.context.columnType != ColumnType.COMPUTED)
                .forEach(parsingResult -> builder.addField(buildField(parsingResult)));

        return builder.build();
    }

    private static final FieldSpec buildField(FieldMetaSignature fieldMetaSignature) {
        final TypeName typeNameForFunctionParam = TypeUtils.determineTypeForFunctionParam(fieldMetaSignature.sourceType);
        final String fieldName = SNAKE_CASE_NAMING.apply(fieldMetaSignature.context.fieldName).toUpperCase();
        final String cqlColumn = fieldMetaSignature.context.quotedCqlColumn;
        return FieldSpec.builder(typeNameForFunctionParam, fieldName, Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("<br/>\n")
                .addJavadoc("Field to be used for <em>manager.dsl().select().function(...)</em> call\n")
                .addJavadoc("<br/>\n")
                .addJavadoc("This is an alias for the field <strong>$S</strong>", fieldMetaSignature.context.fieldName)
                .initializer(CodeBlock
                        .builder()
                        .add("new $T($T.empty()){\n", typeNameForFunctionParam, OPTIONAL)
                        .add("  @$T\n", OVERRIDE_ANNOTATION)
                        .beginControlFlow("  protected String cqlColumn()")
                        .addStatement("    return $S", cqlColumn)
                        .endControlFlow()
                        .add("  @$T\n", OVERRIDE_ANNOTATION)
                        .beginControlFlow("  public boolean isFunctionCall()")
                        .addStatement("    return false")
                        .endControlFlow()
                        .add("  }\n")
                        .build()
                )
                .build();
    }


}
