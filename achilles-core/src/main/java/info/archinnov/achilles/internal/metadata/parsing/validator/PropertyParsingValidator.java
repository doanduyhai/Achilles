/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.parsing.validator;

import static info.archinnov.achilles.internal.metadata.holder.PropertyMeta.GET_CQL_COLUMN_NAME;
import static info.archinnov.achilles.type.ConsistencyLevel.ANY;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.parsing.PropertyParser;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyParsingValidator {
    private static final Logger log = LoggerFactory.getLogger(PropertyParsingValidator.class);


    public void validateNoDuplicatePropertyName(PropertyParsingContext context) {
        String propertyName = context.getCurrentPropertyName();
        Validator.validateBeanMappingFalse(context.getPropertyMetas().containsKey(propertyName),
                "The property '%s' is already used for the entity '%s'", propertyName, context.getCurrentEntityClass()
                        .getCanonicalName());
    }

    public void validateNoDuplicateCQLName(PropertyParsingContext context) {
        String currentCQL3ColumnName = context.getCurrentCQL3ColumnName();
        log.debug("Validate that property name {} is unique for the entity class {}", currentCQL3ColumnName, context
                .getCurrentEntityClass().getCanonicalName());

        final Set<String> distincCQLColumNames = new HashSet<>();
        final List<String> cqlColumnNames = new ArrayList<>(FluentIterable.from(context.getPropertyMetas().values())
                .filter(PropertyType.EXCLUDE_EMBEDDED_ID_TYPE)
                .transform(GET_CQL_COLUMN_NAME).toList());
        final List<String> cqlPrimaryKeyColumnNames = FluentIterable.from(context.getPropertyMetas().values())
                .filter(PropertyType.EMBEDDED_ID_TYPE)
                .first()
                .transform(PropertyMeta.GET_CQL_COLUMN_NAMES_FROM_EMBEDDED_ID)
                .or(new ArrayList<String>());

        cqlColumnNames.addAll(cqlPrimaryKeyColumnNames);

        for (String cqlColumnName : cqlColumnNames) {

            Validator.validateBeanMappingTrue(distincCQLColumNames.add(cqlColumnName),
                    "The CQL column '%s' is already used for the entity '%s'", cqlColumnName, context.getCurrentEntityClass().getCanonicalName());

        }

    }

    public void validateMapGenerics(Field field, Class<?> entityClass) {
        log.debug("Validate parameterized types for property {} of entity class {}", field.getName(),
                entityClass.getCanonicalName());

        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType)) {
            throw new AchillesBeanMappingException("The Map type should be parameterized for the entity '"
                    + entityClass.getCanonicalName() + "'");
        } else {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length <= 1) {
                throw new AchillesBeanMappingException(
                        "The Map type should be parameterized with <K,V> for the entity '"
                                + entityClass.getCanonicalName() + "'");
            }
        }
    }

    public void validateConsistencyLevelForCounter(PropertyParsingContext context,
            Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        log.debug("Validate that counter property {} of entity class {} does not have ANY consistency level",
                context.getCurrentPropertyName(), context.getCurrentEntityClass().getCanonicalName());

        if (consistencyLevels.left == ANY || consistencyLevels.right == ANY) {
            throw new AchillesBeanMappingException(
                    "Counter field '"
                            + context.getCurrentField().getName()
                            + "' of entity '"
                            + context.getCurrentEntityClass().getCanonicalName()
                            + "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
        }
    }

    public void validateIndexIfSet(PropertyParsingContext context) {
        String fieldName = context.getCurrentPropertyName();
        String className = context.getCurrentEntityClass().getCanonicalName();
        log.debug("Validate that this property {} of entity class {} has a properly set index parameter, if set",
                fieldName, className);
        if (PropertyParser.getIndexName(context.getCurrentField()) != null) {

            Validator.validateBeanMappingTrue(PropertyParser.isSupportedType(context.getCurrentField().getType()),
                    "Property '%s' of entity '%s' cannot be indexed because the type '%s' is not supported", fieldName,
                    className, context.getCurrentField().getType().getCanonicalName());
            Validator.validateBeanMappingFalse(context.isEmbeddedId(),
                    "Property '%s' of entity '%s' is part of a compound primary key and therefore cannot be indexed",
                    fieldName, className);

            Validator.validateBeanMappingFalse(context.isPrimaryKey(),
                    "Property '%s' of entity '%s' is a primary key and therefore cannot be indexed", fieldName,
                    className);
        }
    }

//    public static void validateAllowedTypes(Class<?> type, Set<Class<?>> allowedTypes, String message) {
//        log.debug("Validate that type {} is supported", type);
//        if (!allowedTypes.contains(type) && !type.isEnum()) {
//            throw new AchillesBeanMappingException(message);
//        }
//    }

    public static enum Singleton {
        INSTANCE;

        private final PropertyParsingValidator instance = new PropertyParsingValidator();

        public PropertyParsingValidator get() {
            return instance;
        }
    }
}
