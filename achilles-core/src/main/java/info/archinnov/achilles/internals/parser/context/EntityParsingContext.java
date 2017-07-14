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

import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.internals.parser.CodecFactory;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class EntityParsingContext {
    public final TypeElement entityTypeElement;
    public final TypeName entityType;
    public final InternalNamingStrategy namingStrategy;
    public final GlobalParsingContext globalContext;
    public final String className;
    public final Collection<String> optionalSetters;

    public EntityParsingContext(TypeElement elm, TypeName entityType, InternalNamingStrategy namingStrategy, GlobalParsingContext globalContext) {
        this.entityTypeElement = elm;
        this.entityType = entityType;
        this.globalContext = globalContext;
        this.namingStrategy = namingStrategy;
        this.className = entityType.toString();
        this.optionalSetters = ElementFilter.constructorsIn(elm.getEnclosedElements()).stream()
                .map(x -> x.getAnnotation(EntityCreator.class))
                .filter(Objects::nonNull)
                .map(EntityCreator::value)
                .map(v -> Stream.of(v).collect(toSet()))
                .findFirst()
                .orElse(emptySet());
    }

    public boolean hasCodecFor(TypeName typeName) {
        return globalContext.hasCodecFor(typeName);
    }

    public CodecFactory.CodecInfo getCodecFor(TypeName typeName) {
        return globalContext.getCodecFor(typeName);
    }
}
