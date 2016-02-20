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

import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;


public abstract class AbstractBeanParser {

    protected final AptUtils aptUtils;

    protected AbstractBeanParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public List<FieldParser.FieldMetaSignature> parseFields(TypeElement elm, FieldParser fieldParser, GlobalParsingContext context) {
        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(elm, Strategy.class);
        final InternalNamingStrategy namingStrategy = inferNamingStrategy(strategy, context.namingStrategy);

        final EntityParsingContext entityContext = new EntityParsingContext(elm, TypeName.get(aptUtils.erasure(elm)), namingStrategy, context);

        return extractCandidateFields(elm, context.fieldFilter)
                .map(x -> fieldParser.parse(x, entityContext))
                .collect(toList());
    }

    protected Stream<VariableElement> extractCandidateFields(TypeElement elm, FieldFilter fieldFilter) {
        return ElementFilter.fieldsIn(aptUtils.elementUtils.getAllMembers(elm))
                .stream()
                .filter(fieldFilter);
    }

}
