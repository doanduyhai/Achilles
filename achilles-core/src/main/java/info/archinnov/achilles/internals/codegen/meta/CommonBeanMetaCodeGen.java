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

package info.archinnov.achilles.internals.codegen.meta;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import info.archinnov.achilles.annotations.Factory;
import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;

import javax.lang.model.element.Modifier;
import java.util.Optional;
import java.util.stream.Stream;

import static info.archinnov.achilles.internals.parser.TypeUtils.ABSTRACT_PROPERTY;
import static info.archinnov.achilles.internals.parser.TypeUtils.ARRAYS;
import static info.archinnov.achilles.internals.parser.TypeUtils.COLLECTIONS;
import static info.archinnov.achilles.internals.parser.TypeUtils.CONSTRUCTOR;
import static info.archinnov.achilles.internals.parser.TypeUtils.FACTORY;
import static info.archinnov.achilles.internals.parser.TypeUtils.ILLEGAL_ARGUMENT_EXCEPTION;
import static info.archinnov.achilles.internals.parser.TypeUtils.LIST;
import static info.archinnov.achilles.internals.parser.TypeUtils.NAMING_STRATEGY;
import static info.archinnov.achilles.internals.parser.TypeUtils.NO_SUCH_METHOD_EXCEPTION;
import static info.archinnov.achilles.internals.parser.TypeUtils.OPTIONAL;
import static info.archinnov.achilles.internals.parser.TypeUtils.STREAM;
import static info.archinnov.achilles.internals.parser.TypeUtils.WILDCARD;
import static info.archinnov.achilles.internals.parser.TypeUtils.genericType;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.getNamingStrategy;
import static java.util.stream.Collectors.joining;

public interface CommonBeanMetaCodeGen {
    String getClassAccessorName();

    default MethodSpec buildGetStaticNamingStrategy(Optional<Strategy> strategy) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticNamingStrategy")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, NAMING_STRATEGY));

        if (strategy.isPresent()) {
            final InternalNamingStrategy internalStrategy = getNamingStrategy(strategy.get().naming());
            return builder
                    .addStatement("return $T.of(new $L())", OPTIONAL, internalStrategy.FQCN())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    default MethodSpec emptyOption(MethodSpec.Builder builder) {
        return builder
                .addStatement("return $T.empty()", OPTIONAL)
                .build();
    }

    default MethodSpec buildConstructor(final TypeName rawClassTypeName) {
        return MethodSpec.methodBuilder("getConstructor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(CONSTRUCTOR, rawClassTypeName))
                .addStatement("return $T.of(" + getClassAccessorName() + "().getConstructors())\n" +
                                "        .filter(x -> x.isAnnotationPresent($T.class)).findFirst()\n" +
                                "        .map($T.class::cast)\n" +
                                "        .orElseGet(() -> {\n" +
                                "            try {\n" +
                                "                return $T.class.cast(" + getClassAccessorName() + "().getConstructor());\n" +
                                "            } catch ($T e) {\n" +
                                "                throw new $T(\"Invalid class \" + " + getClassAccessorName() + "().getName());\n" +
                                "            }\n" +
                                "         })",
                        STREAM, FACTORY, CONSTRUCTOR, CONSTRUCTOR,
                        NO_SUCH_METHOD_EXCEPTION, ILLEGAL_ARGUMENT_EXCEPTION)
                .build();
    }

    default ParameterizedTypeName propertyListType(TypeName rawClassType) {
        return genericType(LIST, genericType(ABSTRACT_PROPERTY, rawClassType, WILDCARD, WILDCARD));
    }

    default MethodSpec buildConstructorProperties(final TypeName rawClassTypeName, final Factory factory) {
        final MethodSpec.Builder base = MethodSpec.methodBuilder("getConstructorProperties")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(propertyListType(rawClassTypeName));
        if (factory != null && factory.value().length > 0) {
            base.addStatement("return $T.asList($L)", ARRAYS, Stream.of(factory.value()).collect(joining(",")));
        } else {
            base.addStatement("return $T.emptyList()", COLLECTIONS);
        }
        return base.build();
    }
}
