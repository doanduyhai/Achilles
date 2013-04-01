package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.AbstractBatchContext;
import info.archinnov.achilles.entity.context.AbstractBatchContext.BatchType;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

/**
 * EntityValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityValidator
{
	private EntityIntrospector introspector = new EntityIntrospector();
	private EntityProxifier proxifier = new EntityProxifier();

	@SuppressWarnings("rawtypes")
	public void validateEntity(Object entity, Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		Validator.validateNotNull(entity, "Entity should not be null");

		Class baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta<?> entityMeta = entityMetaMap.get(baseClass);
		validateEntity(entity, entityMeta);

	}

	public void validateEntity(Object entity, EntityMeta<?> entityMeta)
	{
		Validator.validateNotNull(entityMeta, "The entity " + entity.getClass().getCanonicalName()
				+ " is not managed by Achilles");

		Object id = introspector.getKey(entity, entityMeta.getIdMeta());
		if (id == null)
		{
			throw new IllegalArgumentException("Cannot get primary key for entity "
					+ entity.getClass().getCanonicalName());
		}
	}

	@SuppressWarnings("unchecked")
	public <T, ID> void validateNotCFDirectMapping(Object entity,
			Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		Validator.validateNotNull(entity, "Entity should not be null");

		Class<T> baseClass = (Class<T>) proxifier.deriveBaseClass(entity);
		EntityMeta<ID> entityMeta = (EntityMeta<ID>) entityMetaMap.get(baseClass);

		if (entityMeta.isColumnFamilyDirectMapping())
		{
			throw new IllegalArgumentException(
					"This operation is not allowed for an entity directly mapped to a native column family");
		}
	}

	public <ID> void validateNoPendingBatch(PersistenceContext<ID> context)
	{
		Validator
				.validateFalse(
						context.isBatchMode(),
						"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	public <ID> void validateNoPendingBatch(AbstractBatchContext context)
	{
		Validator
				.validateFalse(
						context.type() == BatchType.BATCH,
						"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels batch start with 'startBatch(readLevel,writeLevel)'");
	}
}
