/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import static info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry.FORBIDDEN_KEYSPACES;
import static info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry.SYSTEM_FUNCTIONS_NAME;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;

import org.apache.commons.lang3.StringUtils;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.metamodel.functions.UDFSignature;
import info.archinnov.achilles.internals.metamodel.functions.UDFSignature.UDFParamSignature;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;

public class UDFParser {

    public static List<UDFSignature> parseFunctionRegistryAndValidateTypes(AptUtils aptUtils, TypeElement elm) {
        final List<ExecutableElement> methods = ElementFilter.methodsIn(elm.getEnclosedElements());
        final Optional<String> keyspace = AnnotationTree.findKeyspaceForFunctionRegistry(aptUtils, elm);

        final TypeName sourceClass = TypeName.get(aptUtils.erasure(elm));

        //Not allow to declare function in system keyspaces
        if (keyspace.isPresent()) {
            final String keyspaceName = keyspace.get();
            aptUtils.validateFalse(FORBIDDEN_KEYSPACES.contains(keyspaceName) || FORBIDDEN_KEYSPACES.contains(keyspaceName.toLowerCase()),
                    "The provided keyspace '%s' on function registry class '%s' is forbidden because it is a system keyspace",
                    keyspaceName, sourceClass);
        }

        aptUtils.validateFalse(keyspace.isPresent() && isBlank(keyspace.get()),
                "The declared keyspace for function registry '%s' should not be blank", elm.getSimpleName().toString());

        return methods
                .stream()
                .map(method -> {
                    final List<TypeName> parametersType = method.getParameters().stream().map(VariableElement::asType).map(TypeName::get).collect(toList());
                    final String methodName = method.getSimpleName().toString();
                    final List<UDFParamSignature> parameterSignatures = method.getParameters()
                            .stream()
                            .map(x -> new UDFParamSignature(TypeName.get(x.asType()).box(), x.getSimpleName().toString()))
                            .collect(toList());

                    //Validate parameter types
                    for (TypeName param : parametersType) {
                        TypeValidator.validateNativeTypesForFunction(aptUtils, method, param, "argument");
                    }

                    //Validate return type
                    final TypeMirror returnTypeMirror = method.getReturnType();
                    aptUtils.validateFalse(returnTypeMirror.getKind() == TypeKind.VOID,
                        "The return type for the method '%s' on class '%s' should not be VOID",
                        method.toString(), elm.getSimpleName().toString());

                    final TypeName returnType = TypeName.get(returnTypeMirror).box();
                    TypeValidator.validateNativeTypesForFunction(aptUtils, method, returnType, "return type");

                    // Validate NOT system function comparing only name lowercase
                    aptUtils.validateFalse(SYSTEM_FUNCTIONS_NAME.contains(methodName.toLowerCase()),
                            "The name of the function '%s' in class '%s' is reserved for system functions", method, sourceClass);

                    return new UDFSignature(keyspace, sourceClass, methodName, returnType, parametersType, parameterSignatures);
                })
                .collect(toList());
    }

    public static void validateNoDuplicateDeclaration(AptUtils aptUtils, List<UDFSignature> signatures) {
        // Validate not declared many time using full equality
        for (UDFSignature signature : signatures) {
            signatures
                    .stream()
                    .filter(signature::equals) //Equality by comparing name, keyspace, return types and param types
                    .filter(x -> x != signature) //Identity comparison, exclude self
                    .forEach(x -> aptUtils.printError("Functions '%s' and '%s' have same signature. Duplicate function declaration is not allowed",
                            signature, x));
        }
    }
}
