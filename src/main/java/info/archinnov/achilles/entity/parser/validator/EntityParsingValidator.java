package info.archinnov.achilles.entity.parser.validator;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityParsingValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingValidator
{
	private static final Logger log = LoggerFactory.getLogger(EntityParsingValidator.class);

	public void validateHasIdMeta(Class<?> entityClass, PropertyMeta<?, ?> idMeta)
	{
		log.debug("Validate that entity class {} has an id meta", entityClass.getCanonicalName());

		if (idMeta == null)
		{
			throw new AchillesBeanMappingException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}
	}

	public void validatePropertyMetas(EntityParsingContext context)
	{
		log.debug("Validate that there is at least one property meta for the entity class {}",
				context.getCurrentEntityClass().getCanonicalName());

		if (context.getPropertyMetas().isEmpty())
		{
			throw new AchillesBeanMappingException(
					"The entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		}
	}

	public void validateWideRows(EntityParsingContext context)
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = context.getPropertyMetas();
		if (context.isWideRow())
		{
			log.debug(
					"Validate that there is at least one property meta for the wide row class {}",
					context.getCurrentEntityClass().getCanonicalName());

			if (propertyMetas != null && propertyMetas.size() > 1)
			{
				throw new AchillesBeanMappingException("The ColumnFamily entity '"
						+ context.getCurrentEntityClass().getCanonicalName()
						+ "' should not have more than one property annotated with @Column");
			}

			PropertyType type = propertyMetas.entrySet().iterator().next().getValue().type();

			log.debug(
					"Validate that the property meta for the wide row class {} is of type WIDE_MAP or JOIN_WIDE_MAP",
					context.getCurrentEntityClass().getCanonicalName());

			if (type != PropertyType.WIDE_MAP && type != PropertyType.JOIN_WIDE_MAP)
			{
				throw new AchillesBeanMappingException("The ColumnFamily entity '"
						+ context.getCurrentEntityClass().getCanonicalName()
						+ "' should have one and only one @Column/@JoinColumn of type WideMap");
			}
		}
	}

	public void validateJoinEntityNotWideRow(PropertyMeta<?, ?> propertyMeta,
			EntityMeta joinEntityMeta)
	{
		log.debug(
				"Validate that the join entity for the property {} of the entity class {} is not a wide row",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		if (joinEntityMeta.isWideRow())
		{
			throw new AchillesBeanMappingException("The entity '" + joinEntityMeta.getClassName()
					+ "' is a Wide row and cannot be a join entity");
		}
	}

	public void validateJoinEntityExist(Map<Class<?>, EntityMeta> entityMetaMap,
			Class<?> joinEntityClass)
	{
		log.debug("Validate that the join entity class {} exists among all parsed entities",
				joinEntityClass.getCanonicalName());

		if (!entityMetaMap.containsKey(joinEntityClass))
		{
			throw new AchillesBeanMappingException("Cannot find mapping for join entity '"
					+ joinEntityClass.getCanonicalName() + "'");
		}
	}

	public void validateAtLeastOneEntity(List<Class<?>> entities, List<String> entityPackages)
	{
		log.debug("Validate that at least one entity is found in the packages {}",
				StringUtils.join(entityPackages, ","));

		if (entities.isEmpty())
		{
			throw new AchillesBeanMappingException(
					"No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages '"
							+ StringUtils.join(entityPackages, ",") + "'");
		}
	}
}
