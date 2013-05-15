package info.archinnov.achilles.entity.operations;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
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
public class ThriftEntityPersister implements AchillesEntityPersister
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityPersister.class);

	private AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();
	private ThriftEntityLoader loader = new ThriftEntityLoader();
	private ThriftPersisterImpl persisterImpl = new ThriftPersisterImpl();

	@Override
	public <ID> void persist(AchillesPersistenceContext<ID> context)
	{

		EntityMeta<ID> entityMeta = context.getEntityMeta();

		if (!entityMeta.isWideRow())
		{
			log.debug("Persisting transient entity {}", context.getEntity());

			persisterImpl.batchPersistVersionSerialUID((ThriftPersistenceContext<ID>) context);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				this.persistProperty(context, propertyMeta);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(AchillesPersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.debug("Persisting property {} of entity {}", propertyMeta.getPropertyName(),
				context.getEntity());
		ThriftPersistenceContext<ID> thriftContext = (ThriftPersistenceContext<ID>) context;
		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				persisterImpl.batchPersistSimpleProperty(thriftContext, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				batchPersistListProperty(thriftContext, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				batchPersistSetProperty(thriftContext, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				batchPersistMapProperty(thriftContext, propertyMeta);
				break;
			case JOIN_SIMPLE:
				batchPersistJoinEntity(thriftContext, (PropertyMeta<Void, ?>) propertyMeta);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				batchPersistJoinListOrSetProperty(thriftContext,
						(PropertyMeta<Void, ?>) propertyMeta);
				break;
			case JOIN_MAP:
				batchPersistJoinMapProperty(thriftContext, propertyMeta);
				break;
			default:
				break;
		}
	}

	@Override
	public <ID> void remove(AchillesPersistenceContext<ID> context)
	{
		log.debug("Removing entity of class {} and primary key {} ", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		persisterImpl.remove((ThriftPersistenceContext<ID>) context);
	}

	public <ID, V> void removePropertyBatch(ThriftPersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.debug("Removing property {} from entity of class {} and primary key {} ",
				propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
				context.getPrimaryKey());

		persisterImpl.removePropertyBatch(context, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(
			ThriftPersistenceContext<ID> context, V joinEntity, JoinProperties joinProperties)
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
		else if (context.getConfigContext().isEnsureJoinConsistency())
		{

			log.debug("Consistency check for join entity of class {} and primary key {} ", context
					.getEntityClass().getCanonicalName(), context.getPrimaryKey());

			Long joinVersionSerialUID = loader.loadVersionSerialUID(joinId,
					(ThriftGenericEntityDao<JOIN_ID>) context.findEntityDao(joinMeta
							.getColumnFamilyName()));
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
	private <ID, V> void batchPersistListProperty(ThriftPersistenceContext<ID> context,
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
	private <ID, V> void batchPersistSetProperty(ThriftPersistenceContext<ID> context,
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
	private <ID, K, V> void batchPersistMapProperty(ThriftPersistenceContext<ID> context,
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
	private <ID, V> void batchPersistJoinEntity(ThriftPersistenceContext<ID> context,
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
	private <ID, JOIN_ID, V> void batchPersistJoinListOrSetProperty(
			ThriftPersistenceContext<ID> context, PropertyMeta<Void, V> propertyMeta)
	{

		Collection<V> joinCollection = (Collection<V>) introspector.getValueFromField(
				context.getEntity(), propertyMeta.getGetter());
		if (joinCollection != null)
		{
			persisterImpl.batchPersistJoinCollection(context, propertyMeta, joinCollection, this);
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, K, V, JOIN_ID> void batchPersistJoinMapProperty(
			ThriftPersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta)
	{
		Map<K, V> joinMap = (Map<K, V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (joinMap != null)
		{
			persisterImpl.batchPersistJoinMap(context, propertyMeta, joinMap, this);
		}

	}
}
