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

package info.archinnov.achilles.internals.codegen.function.cassandra2_2;

import static info.archinnov.achilles.internals.parser.TypeUtils.SYSTEM_FUNCTIONS_CLASS;

import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.codegen.dsl.JSONFunctionCallSupport;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;

public class FunctionsRegistryCodeGen2_2 extends FunctionsRegistryCodeGen
        implements JSONFunctionCallSupport{

    @Override
    public TypeSpec generateFunctionsRegistryClass(String className, List<FunctionSignature> functionSignatures) {

        final TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC);

        if (className.equals(SYSTEM_FUNCTIONS_CLASS)) {
            builder.addJavadoc("This class is the common registry for all system functions");
            buildAcceptAllMethodsForSystemFunction().forEach(builder::addMethod);
        } else {
            builder.addJavadoc("This class is the common registry for all registered user-defined functions");
        }

        functionSignatures.forEach(signature -> builder.addMethod(super.buildMethodForFunction(signature)));

        return builder.build();
    }

    @Override
    protected List<MethodSpec> buildAcceptAllMethodsForSystemFunction() {
        final List<MethodSpec> methods = super.buildAcceptAllMethodsForSystemFunction();

        methods.add(buildToJSONFunctionCall());

        return methods;
    }
}