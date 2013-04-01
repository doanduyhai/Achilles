package info.archinnov.achilles.entity.parser.validator;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * EntityParsingValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingValidator
{
	public void validateHasIdMeta(Class<?> entityClass, PropertyMeta<Void, ?> idMeta)
	{
		if (idMeta == null)
		{
			throw new AchillesBeanMappingException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}
	}

	public void validatePropertyMetas(EntityParsingContext context)
	{
		if (context.getPropertyMetas().isEmpty())
		{
			throw new AchillesBeanMappingException(
					"The entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		}
	}

	public void validateColumnFamilyDirectMappings(EntityParsingContext context)
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = context.getPropertyMetas();
		if (context.isColumnFamilyDirectMapping())
		{
			if (propertyMetas != null && propertyMetas.size() > 1)
			{
				throw new AchillesBeanMappingException("The ColumnFamily entity '"
						+ context.getCurrentEntityClass().getCanonicalName()
						+ "' should not have more than one property annotated with @Column");
			}

			PropertyType type = propertyMetas.entrySet().iterator().next().getValue().type();

			if (type != PropertyType.EXTERNAL_WIDE_MAP
					&& type != PropertyType.EXTERNAL_JOIN_WIDE_MAP)
			{
				throw new AchillesBeanMappingException("The ColumnFamily entity '"
						+ context.getCurrentEntityClass().getCanonicalName()
						+ "' should have one and only one @Column/@JoinColumn of type WideMap");
			}
		}
	}

	public <ID> void validateJoinEntityNotDirectCFMapping(EntityMeta<ID> joinEntityMeta)
	{
		if (joinEntityMeta.isColumnFamilyDirectMapping())
		{
			throw new AchillesBeanMappingException("The entity '" + joinEntityMeta.getClassName()
					+ "' is a direct Column Family mapping and cannot be a join entity");
		}
	}

	public <JOIN_ID> void validateJoinEntityExist(Map<Class<?>, EntityMeta<?>> entityMetaMap,
			Class<JOIN_ID> joinEntityClass)
	{

		if (!entityMetaMap.containsKey(joinEntityClass))
		{
			throw new AchillesBeanMappingException("Cannot find mapping for join entity '"
					+ joinEntityClass.getCanonicalName() + "'");
		}
	}

	public void validateAtLeastOneEntity(List<Class<?>> entities, List<String> entityPackages)
	{
		if (entities.isEmpty())
		{
			throw new AchillesBeanMappingException(
					"No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages "
							+ StringUtils.join(entityPackages, ","));
		}
	}
}
