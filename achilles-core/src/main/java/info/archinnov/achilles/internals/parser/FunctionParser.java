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

package info.archinnov.achilles.internals.parser;

import static info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry.FORBIDDEN_KEYSPACES;
import static info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry.SYSTEM_FUNCTIONS_NAME;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.context.FunctionSignature.FunctionParamSignature;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class FunctionParser {

    public static List<FunctionSignature> parseFunctionRegistryAndValidateTypes(AptUtils aptUtils, TypeElement elm, GlobalParsingContext context) {
        final List<ExecutableElement> methods = ElementFilter.methodsIn(elm.getEnclosedElements());
        final Optional<String> keyspace = AnnotationTree.findKeyspaceForFunctionRegistry(aptUtils, elm);
        final FunctionParamParser paramParser = new FunctionParamParser(aptUtils);

        final TypeName parentType = TypeName.get(aptUtils.erasure(elm));

        //Not allow to declare function in system keyspaces
        if (keyspace.isPresent()) {
            final String keyspaceName = keyspace.get();
            aptUtils.validateFalse(FORBIDDEN_KEYSPACES.contains(keyspaceName) || FORBIDDEN_KEYSPACES.contains(keyspaceName.toLowerCase()),
                    "The provided keyspace '%s' on function registry class '%s' is forbidden because it is a system keyspace",
                    keyspaceName, parentType);
        }

        aptUtils.validateFalse(keyspace.isPresent() && isBlank(keyspace.get()),
                "The declared keyspace for function registry '%s' should not be blank", elm.getSimpleName().toString());

        return methods
                .stream()
                .map(method -> {
                    final String methodName = method.getSimpleName().toString();
                    final List<AnnotationTree> annotationTrees = AnnotationTree.buildFromMethodForParam(aptUtils, method);
                    final List<? extends VariableElement> parameters = method.getParameters();
                    final List<FunctionParamSignature> parameterSignatures = new ArrayList<>(parameters.size());
                    for (int i = 0; i< parameters.size(); i++) {
                        final VariableElement parameter = parameters.get(i);
                        context.nestedTypesValidator().validate(aptUtils, annotationTrees.get(i), method.toString(), parentType);
                        final FunctionParamSignature functionParamSignature = paramParser
                                .parseParam(context, annotationTrees.get(i), parentType, methodName, parameter.getSimpleName().toString());
                        parameterSignatures.add(functionParamSignature);
                    }

                    //Validate return type
                    final TypeMirror returnType = method.getReturnType();
                    aptUtils.validateFalse(returnType.getKind() == TypeKind.VOID,
                        "The return type for the method '%s' on class '%s' should not be VOID",
                        method.toString(), elm.getSimpleName().toString());

                    aptUtils.validateFalse(returnType.getKind().isPrimitive(),
                        "Due to internal JDK API limitations, UDF/UDA return types cannot be primitive. " +
                        "Use their Object counterpart instead for method '%s' " +
                        "in function registry '%s'", method.toString(), elm.getQualifiedName());

                    final FunctionParamSignature returnTypeSignature = paramParser.parseParam(context,
                            AnnotationTree.buildFromMethodForReturnType(aptUtils, method),
                            parentType, methodName, "returnType");

                    // Validate NOT system function comparing only name lowercase
                    aptUtils.validateFalse(SYSTEM_FUNCTIONS_NAME.contains(methodName.toLowerCase()),
                            "The name of the function '%s' in class '%s' is reserved for system functions", method, parentType);

                    return new FunctionSignature(keyspace, parentType, methodName, returnTypeSignature, parameterSignatures);
                })
                .collect(toList());
    }

    public static void validateNoDuplicateDeclaration(AptUtils aptUtils, List<FunctionSignature> signatures) {
        // Validate not declared many time using full equality
        for (FunctionSignature signature : signatures) {
            signatures
                    .stream()
                    .filter(signature::equals) //Equality by comparing name, keyspace, return types and param types
                    .filter(x -> x != signature) //Identity comparison, exclude self
                    .forEach(x -> aptUtils.printError("Functions '%s' and '%s' have same signature. Duplicate function declaration is not allowed",
                            signature, x));
        }
    }
}
