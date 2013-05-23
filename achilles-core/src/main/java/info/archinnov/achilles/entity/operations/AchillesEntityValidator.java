package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesEntityValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntityValidator
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityValidator.class);

	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();
	private AchillesEntityProxifier proxifier;

	public AchillesEntityValidator(AchillesEntityProxifier proxifier) {
		this.proxifier = proxifier;
	}

	public void validateEntity(Object entity, Map<Class<?>, EntityMeta> entityMetaMap)
	{
		Validator.validateNotNull(entity, "Entity should not be null");

		Class<?> baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta entityMeta = entityMetaMap.get(baseClass);
		validateEntity(entity, entityMeta);

	}

	public void validateEntity(Object entity, EntityMeta entityMeta)
	{
		log.debug("Validate entity {}", entity);

		Validator.validateNotNull(entityMeta, "The entity " + entity.getClass().getCanonicalName()
				+ " is not managed by Achilles");

		Object id = invoker.getPrimaryKey(entity, entityMeta.getIdMeta());
		if (id == null)
		{
			throw new IllegalArgumentException("Cannot get primary key for entity "
					+ entity.getClass().getCanonicalName());
		}
	}

	public void validateNotWideRow(Object entity, Map<Class<?>, EntityMeta> entityMetaMap)
	{
		log.debug("Validate entity {} is not a wide row", entity);

		Validator.validateNotNull(entity, "Entity should not be null");

		Class<?> baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta entityMeta = entityMetaMap.get(baseClass);

		if (entityMeta.isWideRow())
		{
			throw new IllegalArgumentException("This operation is not allowed for the wide row '"
					+ entity.getClass().getCanonicalName());
		}
	}

	public void validateNoPendingBatch(AchillesPersistenceContext context)
	{
		log.debug("Validate no pending batch");
		Validator
				.validateFalse(
						context.isBatchMode(),
						"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}
}
