package info.archinnov.achilles.entity.operations;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityPersister
{
	private static final Logger log = LoggerFactory.getLogger(EntityPersister.class);

	private EntityIntrospector introspector = new EntityIntrospector();
	private EntityLoader loader = new EntityLoader();
	private ThriftPersisterImpl persisterImpl = new ThriftPersisterImpl();

	public <ID> void persist(PersistenceContext<ID> context)
	{

		EntityMeta<ID> entityMeta = context.getEntityMeta();

		if (!entityMeta.isColumnFamilyDirectMapping())
		{
			log.debug("Persisting transient entity {}", context.getEntity());

			persisterImpl.batchPersistVersionSerialUID(context);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				this.persistProperty(context, propertyMeta);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.debug("Persisting property {} of entity {}", propertyMeta.getPropertyName(),
				context.getEntity());

		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				persisterImpl.batchPersistSimpleProperty(context, propertyMeta);
				break;
			case COUNTER:
				persisterImpl.batchPersistCounter(context, (PropertyMeta<Void, Long>) propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				batchPersistListProperty(context, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				batchPersistSetProperty(context, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				batchPersistMapProperty(context, propertyMeta);
				break;
			case JOIN_SIMPLE:
				batchPersistJoinEntity(context, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				batchPersistJoinListOrSetProperty(context, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case JOIN_MAP:
				batchPersistJoinMapProperty(context, propertyMeta);
				break;
			default:
				break;
		}
	}

	public <ID> void remove(PersistenceContext<ID> context)
	{
		log.debug("Removing entity of class {} and primary key {} ", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		persisterImpl.remove(context);
	}

	public <ID, V> void removePropertyBatch(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.debug("Removing property {} from entity of class {} and primary key {} ",
				propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
				context.getPrimaryKey());

		persisterImpl.removePropertyBatch(context, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(PersistenceContext<ID> context,
			V joinEntity, JoinProperties joinProperties)
	{

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) joinProperties.getEntityMeta();
		JOIN_ID joinId = introspector.getKey(joinEntity, joinMeta.getIdMeta());
		Validate.notNull(joinId, "key value for entity '" + joinMeta.getClassName()
				+ "' should not be null");

		Set<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(ALL) || cascadeTypes.contains(PERSIST))
		{
			log.debug("Cascade-persisting entity of class {} and primary key {} ", context
					.getEntityClass().getCanonicalName(), context.getPrimaryKey());

			persist(context);
		}
		else
		{
			log.debug("Consistency check for join entity of class {} and primary key {} ", context
					.getEntityClass().getCanonicalName(), context.getPrimaryKey());

			Long joinVersionSerialUID = loader.loadVersionSerialUID(joinId,
					context.findEntityDao(joinMeta.getColumnFamilyName()));
			Validator
					.validateNotNull(
							joinVersionSerialUID,
							"The entity '"
									+ joinProperties.getEntityMeta().getClassName()
									+ "' with id '"
									+ joinId
									+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		}

		return joinId;

	}

	@SuppressWarnings("unchecked")
	private <ID, V> void batchPersistListProperty(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{
		List<V> list = (List<V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (list != null)
		{
			persisterImpl.batchPersistList(list, context, propertyMeta);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, V> void batchPersistSetProperty(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{
		Set<V> set = (Set<V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (set != null)
		{
			persisterImpl.batchPersistSet(set, context, propertyMeta);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, K, V> void batchPersistMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{

		Map<K, V> map = (Map<K, V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (map != null)
		{
			persisterImpl.batchPersistMap(map, context, propertyMeta);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, V> void batchPersistJoinEntity(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{
		V joinEntity = (V) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (joinEntity != null)
		{
			persisterImpl.batchPersistJoinEntity(context, propertyMeta, joinEntity, this);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, JOIN_ID, V> void batchPersistJoinListOrSetProperty(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{

		Collection<V> joinCollection = (Collection<V>) introspector.getValueFromField(
				context.getEntity(), propertyMeta.getGetter());
		if (joinCollection != null)
		{
			persisterImpl.batchPersistJoinCollection(context, propertyMeta, joinCollection, this);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, K, V, JOIN_ID> void batchPersistJoinMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{
		Map<K, V> joinMap = (Map<K, V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (joinMap != null)
		{
			persisterImpl.batchPersistJoinMap(context, propertyMeta, joinMap, this);
		}

	}
}
