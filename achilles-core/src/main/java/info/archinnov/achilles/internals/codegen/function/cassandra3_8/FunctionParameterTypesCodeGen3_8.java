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

package info.archinnov.achilles.internals.codegen.function.cassandra3_8;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.lang.String.format;

import java.util.List;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.function.FunctionParameterTypesCodeGen;
import info.archinnov.achilles.internals.parser.context.FunctionsContext;
import info.archinnov.achilles.internals.utils.TypeNameHelper;

public class FunctionParameterTypesCodeGen3_8 extends FunctionParameterTypesCodeGen {

    @Override
    public List<TypeSpec> buildParameterTypesClasses(FunctionsContext functionContext) {
        return buildParameterTypesClassesInternal(functionContext);
    }

    @Override
    protected void enhanceGeneratedType(TypeSpec.Builder builder, TypeName typeName) {
        TypeName returnType = ClassName.get(FUNCTION_PACKAGE, TypeNameHelper.asString(typeName)+ FUNCTION_TYPE_SUFFIX);
        final MethodSpec wrapMethod = MethodSpec.methodBuilder("wrap")
                .addJavadoc("Wrap value of type $T", typeName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addParameter(typeName, "wrappedValue", Modifier.FINAL)
                .returns(returnType)
                .addStatement("$T.validateNotNull(wrappedValue, $S)", VALIDATOR,
                        format("The provided value for wrapper class %s should not be null", returnType))
                .addStatement("return new $T($T.of(wrappedValue))", returnType, OPTIONAL)
                .build();

        builder.addMethod(wrapMethod);
    }

}
