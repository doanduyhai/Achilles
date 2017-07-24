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

import static info.archinnov.achilles.internals.parser.TypeUtils.getRawType;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static info.archinnov.achilles.validation.Validator.validateBeanMappingTrue;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.ArrayUtils.isEmpty;

import java.util.*;
import java.util.stream.Stream;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;


public abstract class AbstractBeanParser {

    protected final AptUtils aptUtils;

    protected AbstractBeanParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public List<FieldMetaSignature> parseFields(TypeElement elm, FieldParser fieldParser,
                                                List<AccessorsExclusionContext> accessorsExclusionContexts,
                                                GlobalParsingContext context) {
        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(elm, Strategy.class);
        final InternalNamingStrategy namingStrategy = inferNamingStrategy(strategy, context.namingStrategy);

        final EntityParsingContext entityContext = new EntityParsingContext(elm, TypeName.get(aptUtils.erasure(elm)), namingStrategy, accessorsExclusionContexts, context);

        return extractCandidateFields(elm, context.fieldFilter)
                .map(x -> fieldParser.parse(x, entityContext))
                .collect(toList());
    }

    protected Stream<VariableElement> extractCandidateFields(TypeElement elm, FieldFilter fieldFilter) {
        return ElementFilter.fieldsIn(aptUtils.elementUtils.getAllMembers(elm))
                .stream()
                .filter(fieldFilter);
    }

    public List<FieldMetaSignature> parseCustomConstructor(String entityClass, TypeElement elm, List<FieldMetaSignature> fieldMetaSignatures) {

        final List<String> fieldNames = fieldMetaSignatures.stream().map(x -> x.context.fieldName).collect(toList());
        final Map<String, FieldMetaSignature> fieldMetaByFieldName = fieldMetaSignatures.stream().collect(toMap(x -> x.context.fieldName, x -> x));

        final Optional<ExecutableElement> customConstructorOptional = ElementFilter.constructorsIn(elm.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getAnnotation(EntityCreator.class) != null)
                .findFirst();

        if (!customConstructorOptional.isPresent()) {
            return Collections.emptyList();
        } else {
            final ExecutableElement customConstructor = customConstructorOptional.get();
            final List<? extends VariableElement> parameters = customConstructor.getParameters();
            final EntityCreator annotation = customConstructor.getAnnotation(EntityCreator.class);

            if (isEmpty(annotation.value())) {

                for (VariableElement param : parameters) {
                    final String paramName = param.getSimpleName().toString();
                    final TypeName paramBoxedType = TypeName.get(param.asType()).box();

                    validateBeanMappingTrue(fieldNames.contains(paramName),
                            "Cannot find matching field name for parameter '%s' of @EntityCreator constructor on entity '%s'",
                            paramName, entityClass);
                    final TypeName sourceType = fieldMetaByFieldName.get(paramName).sourceType.box();
                    validateBeanMappingTrue(sourceType.equals(paramBoxedType),
                            "The type of parameter '%s' of @EntityCreator constructor on entity '%s' is wrong, it should be '%s'",
                            paramName, entityClass, sourceType.toString());
                }

                return parameters
                        .stream()
                        .map(param -> fieldMetaByFieldName.get(param.getSimpleName().toString()))
                        .collect(toList());

            } else {

                final List<String> matchingFieldNames = Arrays.asList(annotation.value());

                validateBeanMappingTrue(matchingFieldNames.size() == parameters.size(),
                        "There should be '%s' declared field name in the @EntityCreator annotation for the entity '%s'",
                        parameters.size(), entityClass);

                for(int i=0; i< parameters.size(); i++) {
                    final TypeName parameterBoxedType = TypeName.get(parameters.get(i).asType()).box();
                    final String matchingFieldName = matchingFieldNames.get(i);

                    validateBeanMappingTrue(fieldNames.contains(matchingFieldName),
                            "Cannot find matching field name for declared field '%s' on @EntityCreator annotation on entity '%s'",
                            matchingFieldName, entityClass);

                    final TypeName sourceType = fieldMetaByFieldName.get(matchingFieldName).sourceType.box();

                    validateBeanMappingTrue(sourceType.equals(parameterBoxedType),
                            "The type of declared parameter '%s' on @EntityCreator annotation of entity '%s' is wrong, it should be '%s'",
                            matchingFieldName, entityClass, sourceType.toString());
                }

                return matchingFieldNames
                        .stream()
                        .map(fieldName -> fieldMetaByFieldName.get(fieldName))
                        .collect(toList());
            }
        }
    }

    public List<AccessorsExclusionContext> prebuildAccessorsExclusion(TypeElement elm, GlobalParsingContext context) {

        final TypeName rawClassTypeName = getRawType(TypeName.get(elm.asType()));

        context.beanValidator().validateConstructor(aptUtils, rawClassTypeName, elm);

        final Optional<ExecutableElement> customConstructorOptional = ElementFilter.constructorsIn(elm.getEnclosedElements())
                .stream()
                .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)) // public constructor
                .filter(x -> x.getAnnotation(EntityCreator.class) != null)
                .findFirst();

        if (!customConstructorOptional.isPresent()) {
            return Collections.emptyList();
        } else {
            final ExecutableElement customConstructor = customConstructorOptional.get();
            final List<? extends VariableElement> parameters = customConstructor.getParameters();
            final EntityCreator annotation = customConstructor.getAnnotation(EntityCreator.class);

            if (isEmpty(annotation.value())) {
                return parameters
                        .stream()
                        .map(param -> new AccessorsExclusionContext(param.getSimpleName().toString(), false, true))
                        .collect(toList());
            } else {
                return Arrays.asList(annotation.value())
                        .stream()
                        .map(fieldName -> new AccessorsExclusionContext(fieldName, false, true))
                        .collect(toList());
            }
        }
    }

}
