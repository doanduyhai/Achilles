package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.validation.Validator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityParsingValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingValidator {
    private static final Logger log = LoggerFactory.getLogger(EntityParsingValidator.class);

    public void validateHasIdMeta(Class<?> entityClass, PropertyMeta<?, ?> idMeta) {
        log.debug("Validate that entity class {} has an id meta", entityClass.getCanonicalName());

        if (idMeta == null) {
            throw new AchillesBeanMappingException(
                    "The entity '"
                            + entityClass.getCanonicalName()
                            + "' should have at least one field with javax.persistence.Id/javax.persistence.EmbeddedId annotation");
        }
    }

    public void validatePropertyMetas(EntityParsingContext context) {
        log.debug("Validate that there is at least one property meta for the entity class {}", context
                .getCurrentEntityClass().getCanonicalName());

        if (context.getPropertyMetas().isEmpty()) {
            throw new AchillesBeanMappingException(
                    "The entity '"
                            + context.getCurrentEntityClass().getCanonicalName()
                            + "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
        }
    }

    public void validateClusteredEntities(EntityParsingContext context) {
        Map<String, PropertyMeta<?, ?>> propertyMetas = context.getPropertyMetas();

        if (context.isClusteredEntity() && context.isThriftImpl()) {
            log.debug("Validate that there is at least one property meta for the clustered entity {}", context
                    .getCurrentEntityClass().getCanonicalName());

            if (propertyMetas != null && propertyMetas.size() != 2) {
                throw new AchillesBeanMappingException("The clustered entity '"
                        + context.getCurrentEntityClass().getCanonicalName()
                        + "' should not have more than two properties annotated with @EmbeddedId/@Column/@JoinColumn");
            }

            Iterator<Entry<String, PropertyMeta<?, ?>>> metaIter = propertyMetas.entrySet().iterator();
            PropertyType type1 = metaIter.next().getValue().type();
            PropertyType type2 = metaIter.next().getValue().type();

            log.debug("Validate that the clustered entity {} has an @EmbeddedId", context.getCurrentEntityClass()
                    .getCanonicalName());

            Validator.validateBeanMappingTrue(type1 == PropertyType.EMBEDDED_ID, "The clustered entity '"
                    + context.getCurrentEntityClass().getCanonicalName() + "' should have an @EmbeddedId property");

            log.debug("Validate that the clustered entity {} has a valid clustered value type", context
                    .getCurrentEntityClass().getCanonicalName());

            Validator.validateBeanMappingTrue(type2.isValidClusteredValueType(), "The clustered entity '"
                    + context.getCurrentEntityClass().getCanonicalName()
                    + "' should have a single @Column/@JoinColumn property of type simple/join simple/counter");
        }
    }

    public void validateJoinEntityNotClusteredEntity(PropertyMeta<?, ?> propertyMeta, EntityMeta joinEntityMeta) {
        log.debug("Validate that the join entity for the property {} of the entity class {} is not clustered",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

        if (joinEntityMeta.isClusteredEntity()) {
            throw new AchillesBeanMappingException("The entity '" + joinEntityMeta.getClassName()
                    + "' is a clustered entity and cannot be a join entity");
        }
    }

    public void validateJoinEntityExist(Map<Class<?>, EntityMeta> entityMetaMap, Class<?> joinEntityClass) {
        log.debug("Validate that the join entity class {} exists among all parsed entities",
                joinEntityClass.getCanonicalName());

        if (!entityMetaMap.containsKey(joinEntityClass)) {
            throw new AchillesBeanMappingException("Cannot find mapping for join entity '"
                    + joinEntityClass.getCanonicalName() + "'");
        }
    }

    public void validateAtLeastOneEntity(List<Class<?>> entities, List<String> entityPackages) {
        log.debug("Validate that at least one entity is found in the packages {}",
                StringUtils.join(entityPackages, ","));

        if (entities.isEmpty()) {
            throw new AchillesBeanMappingException(
                    "No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages '"
                            + StringUtils.join(entityPackages, ",") + "'");
        }
    }
}
