package info.archinnov.achilles.entity.operations;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityPersister implements EntityPersister<ThriftPersistenceContext>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityPersister.class);

	private ReflectionInvoker invoker = new ReflectionInvoker();
	private ThriftEntityLoader loader = new ThriftEntityLoader();
	private ThriftPersisterImpl persisterImpl = new ThriftPersisterImpl();

	@Override
	public void persist(ThriftPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Object entity = context.getEntity();

		if (context.addToProcessingList(entity))
		{
			log.debug("Persisting transient entity {}", entity);
			if (context.isClusteredEntity())
			{
				persistClusteredEntity(context);
			}
			else
			{
				// Remove first
				persisterImpl.removeEntityBatch(context);

				for (PropertyMeta<?, ?> propertyMeta : entityMeta.getPropertyMetas().values())
				{
					this.persistPropertyBatch(context, propertyMeta);
				}
			}
		}
	}

	public void persistPropertyBatch(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		log.debug("Persisting property {} of entity {}", propertyMeta.getPropertyName(),
				context.getEntity());
		switch (propertyMeta.type())
		{
			case ID:
			case SIMPLE:
			case LAZY_SIMPLE:
				persisterImpl.batchPersistSimpleProperty(context, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				batchPersistListProperty(context, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				batchPersistSetProperty(context, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				batchPersistMapProperty(context, propertyMeta);
				break;
			case JOIN_SIMPLE:
				batchPersistJoinEntity(context, propertyMeta);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				batchPersistJoinListOrSetProperty(context, propertyMeta);
				break;
			case JOIN_MAP:
				batchPersistJoinMapProperty(context, propertyMeta);
				break;
			default:
				break;
		}
	}

	@Override
	public void remove(ThriftPersistenceContext context)
	{
		log.debug("Removing entity of class {} and primary key {} ", context
				.getEntityClass()
				.getCanonicalName(),
				context.getPrimaryKey());
		EntityMeta meta = context.getEntityMeta();

		if (meta.isClusteredEntity())
		{
			removeClusteredEntity(context);
		}
		else
		{
			persisterImpl.remove(context);
		}
	}

	public void removePropertyBatch(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		log.debug("Removing property {} from entity of class {} and primary key {} ",
				propertyMeta.getPropertyName(),
				context.getEntityClass().getCanonicalName(), context.getPrimaryKey());

		persisterImpl.removePropertyBatch(context, propertyMeta);
	}

	public Object cascadePersistOrEnsureExists(ThriftPersistenceContext context, Object joinEntity,
			JoinProperties joinProperties)
	{

		EntityMeta joinMeta = joinProperties.getEntityMeta();
		Object joinId = invoker.getPrimaryKey(joinEntity, joinMeta.getIdMeta());
		Validate.notNull(joinId, "key value for entity '" + joinMeta.getClassName()
				+ "' should not be null");

		Set<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(ALL) || cascadeTypes.contains(PERSIST))
		{
			log.debug("Cascade-persisting entity of class {} and primary key {} ", context
					.getEntityClass()
					.getCanonicalName(), context.getPrimaryKey());

			persist(context);
		}
		else if (context.getConfigContext().isEnsureJoinConsistency())
		{

			log.debug("Consistency check for join entity of class {} and primary key {} ", context
					.getEntityClass()
					.getCanonicalName(), context.getPrimaryKey());

			Object primaryKey = loader.loadPrimaryKey(context, joinMeta.getIdMeta());

			Validator
					.validateNotNull(
							primaryKey,
							"The entity '"
									+ joinProperties.getEntityMeta().getClassName()
									+ "' with id '"
									+ joinId
									+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		}

		return joinId;

	}

	public void persistClusteredValue(ThriftPersistenceContext context, Object clusteredValue)
	{
		Object primaryKey = context.getPrimaryKey();
		EntityMeta entityMeta = context.getEntityMeta();
		Object partitionKey = invoker.getPartitionKey(primaryKey, entityMeta.getIdMeta());
		persisterImpl.persistClusteredValueBatch(context, partitionKey, clusteredValue, this);
	}

	private void batchPersistListProperty(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		List<?> list = (List<?>) invoker.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (list != null)
		{
			persisterImpl.batchPersistList(list, context, propertyMeta);
		}
	}

	private void batchPersistSetProperty(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Set<?> set = (Set<?>) invoker.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (set != null)
		{
			persisterImpl.batchPersistSet(set, context, propertyMeta);
		}
	}

	private void batchPersistMapProperty(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Map<?, ?> map = (Map<?, ?>) invoker.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (map != null)
		{
			persisterImpl.batchPersistMap(map, context, propertyMeta);
		}
	}

	private void batchPersistJoinEntity(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Object joinEntity = invoker
				.getValueFromField(context.getEntity(), propertyMeta.getGetter());

		if (joinEntity != null)
		{
			persisterImpl.batchPersistJoinEntity(context, propertyMeta, joinEntity, this);
		}
	}

	private void batchPersistJoinListOrSetProperty(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{

		Collection<?> joinCollection = (Collection<?>) invoker.getValueFromField(
				context.getEntity(),
				propertyMeta.getGetter());
		if (joinCollection != null)
		{
			persisterImpl.batchPersistJoinCollection(context, propertyMeta, joinCollection, this);
		}
	}

	private void batchPersistJoinMapProperty(ThriftPersistenceContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Map<?, ?> joinMap = (Map<?, ?>) invoker.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (joinMap != null)
		{
			persisterImpl.batchPersistJoinMap(context, propertyMeta, joinMap, this);
		}

	}

	private void persistClusteredEntity(ThriftPersistenceContext context)
	{
		Object entity = context.getEntity();
		Object compoundKey = context.getPrimaryKey();
		String className = context.getEntityClass().getCanonicalName();

		Validator.validateNotNull(compoundKey,
				"Compound key should be provided for clustered entity '" + className
						+ "' persistence");
		Validator.validateNotNull(entity, "Entity should be provided for clustered entity '"
				+ className
				+ "' persistence");

		EntityMeta meta = context.getEntityMeta();
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();
		PropertyMeta<?, ?> pm = meta.getFirstMeta();

		Object partitionKey = invoker.getPartitionKey(compoundKey, idMeta);
		Object clusteredValue = invoker.getValueFromField(entity, pm.getGetter());

		Validator.validateNotNull(clusteredValue, "Property '" + pm.getPropertyName()
				+ "' should not be null for clustered entity '" + className + "' persistence");
		persisterImpl.persistClusteredEntity(this, context, partitionKey, clusteredValue);
	}

	private void removeClusteredEntity(ThriftPersistenceContext context)
	{
		Object entity = context.getEntity();
		Object compoundKey = context.getPrimaryKey();
		String className = context.getEntityClass().getCanonicalName();

		Validator.validateNotNull(compoundKey,
				"Compound key should be provided for clustered entity '" + className
						+ "' persistence");
		Validator.validateNotNull(entity, "Entity should be provided for clustered entity '"
				+ className
				+ "' persistence");

		PropertyMeta<?, ?> idMeta = context.getIdMeta();

		Object partitionKey = invoker.getPartitionKey(compoundKey, idMeta);

		persisterImpl.removeClusteredEntity(context, partitionKey);
	}
}
