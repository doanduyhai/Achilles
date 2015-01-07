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
package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.internal.metadata.holder.EntityMetaBuilder.entityMetaBuilder;
import java.lang.reflect.Field;
import java.util.List;

import info.archinnov.achilles.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.validator.EntityParsingValidator;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.InsertStrategy;

public class EntityParser {
    private static final Logger log = LoggerFactory.getLogger(EntityParser.class);

    private EntityParsingValidator validator = EntityParsingValidator.Singleton.INSTANCE.get();
    private PropertyParser parser = PropertyParser.Singleton.INSTANCE.get();
    private PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();
    private EntityIntrospector introspector = EntityIntrospector.Singleton.INSTANCE.get();

    public EntityMeta parseEntity(EntityParsingContext context) {
        log.debug("Parsing entity class {}", context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        validateEntityAndGetObjectMapper(context);

        String keyspaceName = context.getKeyspaceName();
        String tableName = context.getTableName();
        String tableComment = introspector.inferTableComment(entityClass, "Create table for entity \"" + entityClass.getName() + "\"");
        final InsertStrategy insertStrategy = introspector.getInsertStrategy(entityClass, context);


        PropertyMeta idMeta = null;
        List<Field> inheritedFields = introspector.getInheritedPrivateFields(entityClass);
        for (Field field : inheritedFields) {
            PropertyParsingContext propertyContext = context.newPropertyContext(field);
            if (filter.hasAnnotation(field, Id.class) || filter.hasAnnotation(field, PartitionKey.class)) {
                propertyContext.setPrimaryKey(true);
                idMeta = parser.parse(propertyContext);
            } else if (filter.hasAnnotation(field, EmbeddedId.class) || filter.hasAnnotation(field, CompoundPrimaryKey.class)) {
                propertyContext.setCompoundPrimaryKey(true);
                idMeta = parser.parse(propertyContext);
            } else if (filter.hasAnnotation(field, Column.class)) {
                parser.parse(propertyContext);
            } else {
                log.trace("Un-mapped field {} of entity {} will not be managed by Achilles", field.getName(), context
                        .getCurrentEntityClass().getCanonicalName());
            }
        }

        // First validate id meta
        validator.validateHasIdMeta(entityClass, idMeta);

        // Deferred counter property meta completion
        completeCounterPropertyMeta(context, idMeta);

        EntityMeta entityMeta = entityMetaBuilder(idMeta).entityClass(entityClass)
                .className(entityClass.getCanonicalName())
                .keyspaceName(keyspaceName)
                .tableName(tableName).tableComment(tableComment)
                .propertyMetas(context.getPropertyMetas()).consistencyLevels(context.getCurrentConsistencyLevels())
                .insertStrategy(insertStrategy)
                .schemaUpdateEnabled(context.isSchemaUpdateEnabled(keyspaceName, tableName))
                .build();

        validator.validateStaticColumns(entityMeta,idMeta);
        log.trace("Entity meta built for entity class {} : {}", context.getCurrentEntityClass().getCanonicalName(),entityMeta);

        return entityMeta;
    }

    private void validateEntityAndGetObjectMapper(EntityParsingContext context) {

        Class<?> entityClass = context.getCurrentEntityClass();
        log.debug("Validate entity {}", entityClass.getCanonicalName());

        Validator.validateInstantiable(entityClass);

        ObjectMapper objectMapper = context.getObjectMapperFactory().getMapper(entityClass);
        Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '%s'",
                entityClass.getCanonicalName());

        log.debug("Set default object mapper {} for entity {}", objectMapper.getClass().getCanonicalName(),
                entityClass.getCanonicalName());
        context.setCurrentObjectMapper(objectMapper);
    }

    private void completeCounterPropertyMeta(EntityParsingContext context, PropertyMeta idMeta) {
        for (PropertyMeta counterMeta : context.getCounterMetas()) {

            log.debug("Add id Meta {} to counter meta {} of entity class {}", idMeta.getPropertyName(),
                    counterMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName());

            counterMeta.setIdMetaForCounterProperties(idMeta);
        }
    }

    public static enum Singleton {
        INSTANCE;

        private final EntityParser instance = new EntityParser();

        public EntityParser get() {
            return instance;
        }
    }
}
