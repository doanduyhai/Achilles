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

import static info.archinnov.achilles.internals.parser.TypeUtils.getRawType;
import static info.archinnov.achilles.internals.parser.context.ConstructorInfo.ConstructorType.*;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.ArrayUtils.isEmpty;

import java.util.*;
import java.util.stream.Stream;
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
import info.archinnov.achilles.internals.parser.context.ConstructorInfo;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
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

    public List<FieldMetaSignature> parseAndValidateCustomConstructor(BeanValidator beanValidator, String entityClass, TypeElement elm, List<FieldMetaSignature> fieldMetaSignatures) {

        final List<String> fieldNames = fieldMetaSignatures.stream().map(x -> x.context.fieldName).collect(toList());
        final Map<String, FieldMetaSignature> fieldMetaByFieldName = fieldMetaSignatures.stream().collect(toMap(x -> x.context.fieldName, x -> x));

        final TypeName rawClassTypeName = getRawType(TypeName.get(elm.asType()));
        final ConstructorInfo constructorInfo = beanValidator.validateConstructor(aptUtils, rawClassTypeName, elm);

        if (constructorInfo.type == IMMUTABLE) {
            final List<? extends VariableElement> parameters = constructorInfo.constructor.getParameters();
            for (VariableElement param : parameters) {
                final String paramName = param.getSimpleName().toString();
                final TypeName paramBoxedType = TypeName.get(param.asType()).box();

                aptUtils.validateTrue(fieldNames.contains(paramName),
                        "Cannot find matching field name for parameter '%s' of constructor on @Immutable entity '%s'",
                        paramName, entityClass);
                final TypeName boxedSourceType = fieldMetaByFieldName.get(paramName).sourceType.box();
                aptUtils.validateTrue(boxedSourceType.equals(paramBoxedType),
                        "The type of parameter '%s' of constructor on @Immutable entity '%s' is wrong, it should be '%s'",
                        paramName, entityClass, boxedSourceType.toString());

                final VariableElement matchingField = ElementFilter.fieldsIn(aptUtils.elementUtils.getAllMembers(elm))
                        .stream()
                        .filter(element -> element.getSimpleName().toString().equals(paramName))
                        .findFirst()
                        .get();
                final Set<Modifier> modifiers = matchingField.getModifiers();
                aptUtils.validateTrue(modifiers.contains(Modifier.PUBLIC) && modifiers.contains(Modifier.FINAL),
                        "Field '%s' in entity '%s' should have 'public final' modifier because it is an @Immutable entity", paramName, entityClass);
            }

            return parameters
                    .stream()
                    .map(param -> fieldMetaByFieldName.get(param.getSimpleName().toString()))
                    .collect(toList());

        } else if (constructorInfo.type == ENTITY_CREATOR) {

            final List<? extends VariableElement> parameters = constructorInfo.constructor.getParameters();
            final EntityCreator annotation = constructorInfo.constructor.getAnnotation(EntityCreator.class);

            if (isEmpty(annotation.value())) {

                for (VariableElement param : parameters) {
                    final String paramName = param.getSimpleName().toString();
                    final TypeName paramBoxedType = TypeName.get(param.asType()).box();

                    aptUtils.validateTrue(fieldNames.contains(paramName),
                            "Cannot find matching field name for parameter '%s' of @EntityCreator constructor on entity '%s'",
                            paramName, entityClass);
                    final TypeName sourceType = fieldMetaByFieldName.get(paramName).sourceType.box();
                    aptUtils.validateTrue(sourceType.equals(paramBoxedType),
                            "The type of parameter '%s' of @EntityCreator constructor on entity '%s' is wrong, it should be '%s'",
                            paramName, entityClass, sourceType.toString());
                }

                return parameters
                        .stream()
                        .map(param -> fieldMetaByFieldName.get(param.getSimpleName().toString()))
                        .collect(toList());

            } else {

                final List<String> matchingFieldNames = Arrays.asList(annotation.value());

                aptUtils.validateTrue(matchingFieldNames.size() == parameters.size(),
                        "There should be '%s' declared field name in the @EntityCreator annotation for the entity '%s'",
                        parameters.size(), entityClass);

                for (int i = 0; i < parameters.size(); i++) {
                    final TypeName parameterBoxedType = TypeName.get(parameters.get(i).asType()).box();
                    final String matchingFieldName = matchingFieldNames.get(i);

                    aptUtils.validateTrue(fieldNames.contains(matchingFieldName),
                            "Cannot find matching field name for declared field '%s' on @EntityCreator annotation on entity '%s'",
                            matchingFieldName, entityClass);

                    final TypeName sourceType = fieldMetaByFieldName.get(matchingFieldName).sourceType.box();

                    aptUtils.validateTrue(sourceType.equals(parameterBoxedType),
                            "The type of declared parameter '%s' on @EntityCreator annotation of entity '%s' is wrong, it should be '%s'",
                            matchingFieldName, entityClass, sourceType.toString());
                }

                return matchingFieldNames
                        .stream()
                        .map(fieldName -> fieldMetaByFieldName.get(fieldName))
                        .collect(toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

    public List<AccessorsExclusionContext> prebuildAccessorsExclusion(TypeElement elm, GlobalParsingContext context) {

        final TypeName rawClassTypeName = getRawType(TypeName.get(elm.asType()));

        final ConstructorInfo constructorInfo = context.beanValidator().validateConstructor(aptUtils, rawClassTypeName, elm);

        if (constructorInfo.type == DEFAULT) {
            return Collections.emptyList();
        } else if (constructorInfo.type == ENTITY_CREATOR) {
            final List<? extends VariableElement> parameters = constructorInfo.constructor.getParameters();
            final EntityCreator annotation = constructorInfo.constructor.getAnnotation(EntityCreator.class);

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
        // (constructorInfo.type == IMMUTABLE)
        else {
            final List<? extends VariableElement> parameters = constructorInfo.constructor.getParameters();
            return parameters
                    .stream()
                    .map(param -> new AccessorsExclusionContext(param.getSimpleName().toString(), true, true))
                    .collect(toList());
        }
    }

}
