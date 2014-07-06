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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.validator.EntityParsingValidator;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;

public class EntityParser {
    private static final Logger log = LoggerFactory.getLogger(EntityParser.class);

    private EntityParsingValidator validator = new EntityParsingValidator();
    private PropertyParser parser = new PropertyParser();
    private PropertyFilter filter = new PropertyFilter();
    private EntityIntrospector introspector = new EntityIntrospector();

    public EntityMeta parseEntity(EntityParsingContext context) {
        log.debug("Parsing entity class {}", context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        validateEntityAndGetObjectMapper(context);

        String tableName = introspector.inferTableName(entityClass, entityClass.getName());
        Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = introspector.findConsistencyLevels(entityClass, tableName, context.getConfigContext());
        final InsertStrategy insertStrategy = introspector.getInsertStrategy(entityClass, context);

        context.setCurrentConsistencyLevels(consistencyLevels);

        PropertyMeta idMeta = null;
        List<Field> inheritedFields = introspector.getInheritedPrivateFields(entityClass);
        for (Field field : inheritedFields) {
            PropertyParsingContext propertyContext = context.newPropertyContext(field);
            if (filter.hasAnnotation(field, Id.class)) {
                propertyContext.setPrimaryKey(true);
                idMeta = parser.parse(propertyContext);
            } else if (filter.hasAnnotation(field, EmbeddedId.class)) {
                propertyContext.setEmbeddedId(true);
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
                .className(entityClass.getCanonicalName()).columnFamilyName(tableName)
                .propertyMetas(context.getPropertyMetas()).consistencyLevels(context.getCurrentConsistencyLevels())
                .insertStrategy(insertStrategy)
                .schemaUpdateEnabled(context.isSchemaUpdateEnabled(tableName))
                .build();

        validator.validateStaticColumns(entityMeta,idMeta);
        log.trace("Entity meta built for entity class {} : {}", context.getCurrentEntityClass().getCanonicalName(),entityMeta);

        return entityMeta;
    }

    private void validateEntityAndGetObjectMapper(EntityParsingContext context) {

        Class<?> entityClass = context.getCurrentEntityClass();
        log.debug("Validate entity {}", entityClass.getCanonicalName());

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

            counterMeta.getCounterProperties().setIdMeta(idMeta);
        }
    }
}
