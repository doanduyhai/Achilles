package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.CascadeType;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.Validate;

/**
 * EntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityPersister
{

	private EntityIntrospector introspector = new EntityIntrospector();
	private EntityProxifier proxifier = new EntityProxifier();
	private EntityLoader loader = new EntityLoader();

	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory = new DynamicCompositeKeyFactory();
	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	public <ID> void persist(PersistenceContext<ID> context)
	{
		EntityMeta<ID> entityMeta = context.getEntityMeta();

		if (!entityMeta.isColumnFamilyDirectMapping())
		{
			this.batchPersistVersionSerialUID(context);
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

		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.batchPersistSimpleProperty(context, propertyMeta);
				break;
			case COUNTER:
				this.batchPersistCounter(context, (PropertyMeta<Void, Long>) propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.batchPersistListProperty(context, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				this.batchPersistSetProperty(context, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				this.batchPersistMapProperty(context, propertyMeta);
				break;
			case JOIN_SIMPLE:
				this.batchPersistJoinEntity(context, propertyMeta);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				this.batchPersistJoinListOrSetProperty(context, propertyMeta);
				break;
			case JOIN_MAP:
				this.batchPersistJoinMapProperty(context, propertyMeta);
				break;
			default:
				break;
		}
	}

	@SuppressWarnings("unchecked")
	public <ID> void remove(PersistenceContext<ID> context)
	{
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		Mutator<ID> entityMutator = context.getEntityMutator(entityMeta.getColumnFamilyName());
		if (context.isDirectColumnFamilyMapping())
		{
			context.getColumnFamilyDao().removeRowBatch(primaryKey, entityMutator);
		}
		else
		{

			context.getEntityDao().removeRowBatch(primaryKey, entityMutator);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				if (propertyMeta.isExternal())
				{
					ExternalWideMapProperties<ID> externalProperties = (ExternalWideMapProperties<ID>) propertyMeta
							.getExternalWideMapProperties();
					String externalColumnFamilyName = externalProperties
							.getExternalColumnFamilyName();
					GenericColumnFamilyDao<ID, ?> findColumnFamilyDao = context
							.findColumnFamilyDao(externalColumnFamilyName);
					findColumnFamilyDao.removeRowBatch(primaryKey,
							context.getColumnFamilyMutator(externalColumnFamilyName));
				}
				if (propertyMeta.isCounter())
				{
					if (propertyMeta.isWideMap())
					{
						this.removeCounterWideMap(context, (PropertyMeta<Void, Long>) propertyMeta);
					}
					else
					{
						this.removeSimpleCounter(context, (PropertyMeta<Void, Long>) propertyMeta);
					}
				}
			}
		}
	}

	public <ID, V> void removePropertyBatch(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite start = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				ComponentEquality.EQUAL);
		DynamicComposite end = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				GREATER_THAN_EQUAL);
		context.getEntityDao().removeColumnRangeBatch(context.getPrimaryKey(), start, end,
				context.getEntityMutator(context.getColumnFamilyName()));
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
			persist(context);
		}
		else
		{
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

	private <T, ID> void batchPersistVersionSerialUID(PersistenceContext<ID> context)
	{

		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, PropertyType.SERIAL_VERSION_UID.name(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		Long serialVersionUID = introspector.findSerialVersionUID(context.getEntityClass());

		if (serialVersionUID != null)
		{
			context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), composite,
					serialVersionUID.toString(),
					context.getEntityMutator(context.getColumnFamilyName()));
		}
		else
		{
			throw new AchillesException("Cannot find 'serialVersionUID' field for entity class '"
					+ context.getEntityClass().getCanonicalName() + "'");
		}
	}

	private <ID> void batchPersistSimpleProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{
		DynamicComposite name = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		String value = propertyMeta.writeValueToString(introspector.getValueFromField(
				context.getEntity(), propertyMeta.getGetter()));
		if (value != null)
		{
			context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
					context.getEntityMutator(context.getColumnFamilyName()));
		}
	}

	private <ID> void batchPersistListProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{

		List<?> list = (List<?>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		int count = 0;
		if (list != null)
		{
			for (Object value : list)
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, count);

				String stringValue = propertyMeta.writeValueToString(value);
				if (stringValue != null)
				{
					context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							stringValue, context.getEntityMutator(context.getColumnFamilyName()));
				}
				count++;
			}
		}
	}

	private <ID> void batchPersistSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Set<?> set = (Set<?>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, value.hashCode());

				String stringValue = propertyMeta.writeValueToString(value);
				if (stringValue != null)
				{
					context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							stringValue, context.getEntityMutator(context.getColumnFamilyName()));
				}
			}
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
			for (Entry<K, V> entry : map.entrySet())
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, entry.getKey().hashCode());

				String value = propertyMeta.writeValueToString(new KeyValue<K, V>(entry.getKey(),
						entry.getValue()));
				context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
						context.getEntityMutator(context.getColumnFamilyName()));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, JOIN_ID> void batchPersistJoinEntity(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		PropertyMeta<Void, ?> idMeta = propertyMeta.joinIdMeta();
		Object joinEntity = introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (joinEntity != null)
		{
			Object joinId = introspector.getKey(joinEntity, idMeta);
			Validator.validateNotNull(joinId, "Primary key for join entity '" + joinEntity
					+ "' should not be null");
			String joinIdString = idMeta.writeValueToString(joinId);

			DynamicComposite joinComposite = dynamicCompositeKeyFactory
					.createForBatchInsertSingleValue(propertyMeta);
			context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), joinComposite,
					joinIdString, context.getEntityMutator(context.getColumnFamilyName()));

			PersistenceContext<JOIN_ID> joinPersistenceContext = (PersistenceContext<JOIN_ID>) context
					.newPersistenceContext(propertyMeta.joinMeta(), proxifier.unproxy(joinEntity));

			cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity, joinProperties);

		}
	}

	@SuppressWarnings("unchecked")
	private <ID, JOIN_ID> void batchPersistJoinListOrSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{

		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();
		Collection<?> list = (Collection<?>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		int count = 0;
		if (list != null)
		{
			for (Object joinEntity : list)
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, count);

				Object joinEntityId = introspector
						.getValueFromField(joinEntity, idMeta.getGetter());

				String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);
				if (joinEntityIdStringValue != null)
				{
					context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							joinEntityIdStringValue,
							context.getEntityMutator(context.getColumnFamilyName()));

					PersistenceContext<JOIN_ID> joinPersistenceContext = (PersistenceContext<JOIN_ID>) context
							.newPersistenceContext(propertyMeta.joinMeta(),
									proxifier.unproxy(joinEntity));

					this.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity,
							joinProperties);
				}
				count++;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, K, V, JOIN_ID> void batchPersistJoinMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();

		Map<K, V> map = (Map<K, V>) introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (map != null)
		{
			for (Entry<K, V> entry : map.entrySet())
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, entry.getKey().hashCode());

				V joinEntity = entry.getValue();
				Object joinEntityId = introspector
						.getValueFromField(joinEntity, idMeta.getGetter());
				String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);

				String value = propertyMeta.writeValueToString(new KeyValue<K, String>(entry
						.getKey(), joinEntityIdStringValue));
				context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
						context.getEntityMutator(context.getColumnFamilyName()));

				PersistenceContext<JOIN_ID> joinPersistenceContext = (PersistenceContext<JOIN_ID>) context
						.newPersistenceContext(propertyMeta.joinMeta(),
								proxifier.unproxy(joinEntity));

				cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity, joinProperties);
			}
		}

	}

	@SuppressWarnings("unchecked")
	private <ID> void batchPersistCounter(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta)
	{
		String fqcn = propertyMeta.fqcn();
		PropertyMeta<Void, ID> idMeta = (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta();

		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, context.getPrimaryKey(),
				idMeta);
		DynamicComposite comp = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		Object counterValue = introspector.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (counterValue != null)
		{
			insertCounterWithConsistencyLevel(context, propertyMeta, keyComp, comp, counterValue);
		}
	}

	private <ID> void insertCounterWithConsistencyLevel(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta, Composite keyComp, DynamicComposite comp,
			Object counterValue)
	{
		CounterDao dao = context.getCounterDao();
		AchillesConfigurableConsistencyLevelPolicy policy = context.getPolicy();
		boolean resetConsistencyLevel = false;
		if (policy.getCurrentWriteLevel() == null)
		{
			policy.setCurrentWriteLevel(propertyMeta.getWriteConsistencyLevel());
			resetConsistencyLevel = true;
		}
		try
		{
			dao.insertCounterBatch(keyComp, comp, (Long) counterValue, context.getCounterMutator());
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				policy.removeCurrentWriteLevel();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <ID> void removeSimpleCounter(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(),
				context.getPrimaryKey(), (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		DynamicComposite com = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);

		context.getCounterDao().removeCounterBatch(keyComp, com, context.getCounterMutator());
	}

	@SuppressWarnings("unchecked")
	private <ID> void removeCounterWideMap(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(),
				context.getPrimaryKey(), (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		context.getCounterDao().removeCounterRowBatch(keyComp, context.getCounterMutator());
	}
}
