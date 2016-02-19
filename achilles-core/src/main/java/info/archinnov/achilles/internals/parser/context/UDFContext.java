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

package info.archinnov.achilles.internals.parser.context;

import static info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry.EMPTY;
import static info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry.SYSTEM_FUNCTION_REGISTRY;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry;
import info.archinnov.achilles.internals.metamodel.functions.UDFSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;

public class UDFContext {

    public final List<UDFSignature> udfSignatures;
    public final Set<TypeName> allUsedTypes;

    public UDFContext(List<UDFSignature> udfSignatures, Set<TypeName> allUsedTypes) {
        this.udfSignatures = udfSignatures;
        this.allUsedTypes = allUsedTypes;
//        this.parameterTypes = udfSignatures
//                .stream()
//                .flatMap(signature -> signature.parameterTypes.stream())
//                .collect(Collectors.toSet());
    }


    public List<UDFSignature> buildExtraUDFSignatureForSystemFunctionsAcceptingAnyType() {
        final HashSet<TypeName> newSet = new HashSet<>(allUsedTypes);
        newSet.removeAll(TypeUtils.NATIVE_TYPES
                .stream()
                // Exclude collection types
                .filter(x -> (!x.equals(LIST) && !x.equals(SET) && !x.equals(MAP)))
                .collect(toSet()));

        return newSet
                .stream()
                .flatMap(extraType -> {
                    final List<UDFSignature> signatures = new ArrayList<>();
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "token", OBJECT_LONG, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "ttl", OBJECT_INT, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "writetime", OBJECT_LONG, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "countNotNull", BIG_INT, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "min", extraType, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));
                    signatures.add(new UDFSignature(empty(), SYSTEM_FUNCTION_REGISTRY, "max", extraType, EMPTY, asList(new UDFSignature.UDFParamSignature(extraType, "input"))));

                    return signatures.stream();
                })
                .collect(toList());
    }
}
