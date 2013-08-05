package info.archinnov.achilles.entity.parsing;

import static info.archinnov.achilles.entity.metadata.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.entity.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.entity.parsing.validator.EntityParsingValidator;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
    private static final Logger log = LoggerFactory.getLogger(EntityParser.class);

    private EntityParsingValidator validator = new EntityParsingValidator();
    private PropertyParser parser = new PropertyParser();
    private JoinPropertyParser joinParser = new JoinPropertyParser();
    private PropertyFilter filter = new PropertyFilter();
    private EntityIntrospector introspector = new EntityIntrospector();

    public EntityMeta parseEntity(EntityParsingContext context)
    {
        log.debug("Parsing entity class {}", context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        validateEntityAndGetObjectMapper(context);

        String columnFamilyName = introspector.inferColumnFamilyName(entityClass,
                entityClass.getName());
        Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = introspector
                .findConsistencyLevels(entityClass, context.getConfigurableCLPolicy());

        context.setCurrentConsistencyLevels(consistencyLevels);
        context.setCurrentColumnFamilyName(columnFamilyName);

        PropertyMeta<?, ?> idMeta = null;
        List<Field> inheritedFields = introspector.getInheritedPrivateFields(entityClass);
        for (Field field : inheritedFields)
        {
            PropertyParsingContext propertyContext = context.newPropertyContext(field);
            if (filter.hasAnnotation(field, Id.class))
            {
                propertyContext.setPrimaryKey(true);
                idMeta = parser.parse(propertyContext);
            }
            if (filter.hasAnnotation(field, EmbeddedId.class))
            {
                context.setClusteredEntity(true);
                propertyContext.isEmbeddedId(true);
                idMeta = parser.parse(propertyContext);
            }
            else if (filter.hasAnnotation(field, Column.class))
            {
                parser.parse(propertyContext);
            }
            else if (filter.hasAnnotation(field, JoinColumn.class))
            {
                propertyContext.setJoinColumn(true);
                joinParser.parseJoin(propertyContext);
            }
            else
            {
                log.trace("Un-mapped field {} of entity {} will not be managed by Achilles",
                        field.getName(), context.getCurrentEntityClass().getCanonicalName());
            }
        }

        // First validate id meta
        validator.validateHasIdMeta(entityClass, idMeta);

        // Deferred counter property meta completion
        completeCounterPropertyMeta(context, idMeta);

        // Finish validation of property metas
        validator.validatePropertyMetas(context, idMeta);
        validator.validateClusteredEntities(context);

        EntityMeta entityMeta = entityMetaBuilder(idMeta)
                .entityClass(entityClass)
                .className(entityClass.getCanonicalName())
                .columnFamilyName(columnFamilyName)
                .propertyMetas(context.getPropertyMetas())
                .clusteredEntity(context.isClusteredEntity())
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .build();

        saveConsistencyLevel(context, columnFamilyName, consistencyLevels);

        log.trace("Entity meta built for entity class {} : {}", context
                .getCurrentEntityClass()
                .getCanonicalName(), entityMeta);
        return entityMeta;
    }

    public void fillJoinEntityMeta(EntityParsingContext context,
            Map<Class<?>, EntityMeta> entityMetaMap)
    {
        log.debug("Fill in join entity meta into property meta of join type");

        // Retrieve EntityMeta objects for join columns after entities parsing
        for (Entry<PropertyMeta<?, ?>, Class<?>> entry : context
                .getJoinPropertyMetaToBeFilled()
                .entrySet())
        {
            Class<?> clazz = entry.getValue();
            validator.validateJoinEntityExist(entityMetaMap, clazz);

            PropertyMeta<?, ?> propertyMeta = entry.getKey();
            EntityMeta joinEntityMeta = entityMetaMap.get(clazz);

            validator.validateJoinEntityNotClusteredEntity(propertyMeta, joinEntityMeta);

            propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);

            log.trace("Join property meta built for entity class {} : {}",
                    clazz.getCanonicalName(), propertyMeta);
        }
    }

    private void validateEntityAndGetObjectMapper(EntityParsingContext context)
    {

        Class<?> entityClass = context.getCurrentEntityClass();
        log.debug("Validate entity {}", entityClass.getCanonicalName());

        Validator.validateInstantiable(entityClass);

        ObjectMapper objectMapper = context.getObjectMapperFactory().getMapper(entityClass);
        Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '%s'",
                entityClass.getCanonicalName());

        log.debug("Set default object mapper {} for entity {}", objectMapper
                .getClass()
                .getCanonicalName(), entityClass.getCanonicalName());
        context.setCurrentObjectMapper(objectMapper);
    }

    private void completeCounterPropertyMeta(EntityParsingContext context, PropertyMeta<?, ?> idMeta)
    {
        for (PropertyMeta<?, ?> counterMeta : context.getCounterMetas())
        {

            log.debug("Add id Meta {} to counter meta {} of entity class {}", idMeta
                    .getPropertyName(), counterMeta.getPropertyName(), context
                    .getCurrentEntityClass()
                    .getCanonicalName());

            counterMeta.getCounterProperties().setIdMeta(idMeta);
        }
    }

    private void saveConsistencyLevel(EntityParsingContext context, String columnFamilyName,
            Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
    {
        log.debug("Set default read/write consistency levels {} / {} for column family {}",
                consistencyLevels.left.name(), consistencyLevels.right.name(), columnFamilyName);

        context.getConfigurableCLPolicy().setConsistencyLevelForRead(consistencyLevels.left,
                columnFamilyName);
        context.getConfigurableCLPolicy().setConsistencyLevelForWrite(consistencyLevels.right,
                columnFamilyName);
    }

}
