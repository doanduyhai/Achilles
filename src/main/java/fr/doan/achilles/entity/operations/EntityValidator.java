package fr.doan.achilles.entity.operations;

import java.util.Map;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;

/**
 * EntityValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityValidator
{
	private EntityWrapperUtil util = new EntityWrapperUtil();

	@SuppressWarnings("rawtypes")
	public void validateEntity(Object entity, Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		Validator.validateNotNull(entity, "entity");

		Class baseClass = util.deriveBaseClass(entity);
		EntityMeta<?> entityMeta = entityMetaMap.get(baseClass);

		Validator.validateNotNull(entityMeta, "The entity " + entity.getClass().getCanonicalName()
				+ " is not managed");

		Object id = util.determinePrimaryKey(entity, entityMeta);
		if (id == null)
		{
			throw new IllegalArgumentException("Cannot get primary key for entity "
					+ entity.getClass().getCanonicalName());
		}
	}
}
