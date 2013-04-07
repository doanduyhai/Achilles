package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
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
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * ThriftPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersisterImpl
{
	private EntityIntrospector introspector = new EntityIntrospector();
	private EntityProxifier proxifier = new EntityProxifier();

	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory = new DynamicCompositeKeyFactory();
	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	public <T, ID> void batchPersistVersionSerialUID(PersistenceContext<ID> context)
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

	public <ID> void batchPersistSimpleProperty(PersistenceContext<ID> context,
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

	@SuppressWarnings("unchecked")
	public <ID> void batchPersistCounter(PersistenceContext<ID> context,
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

	public <ID, V> void batchPersistList(List<V> list, PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{
		int count = 0;
		for (V value : list)
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

	public <ID, V> void batchPersistSet(Set<V> set, PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta)
	{
		for (V value : set)
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

	public <ID, K, V> void batchPersistMap(Map<K, V> map, PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{
		for (Entry<K, V> entry : map.entrySet())
		{
			DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
					propertyMeta, entry.getKey().hashCode());

			String value = propertyMeta.writeValueToString(new KeyValue<K, V>(entry.getKey(), entry
					.getValue()));
			context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
					context.getEntityMutator(context.getColumnFamilyName()));
		}
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> void batchPersistJoinEntity(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta, V joinEntity, EntityPersister persister)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		PropertyMeta<Void, JOIN_ID> idMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

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

		persister.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity, joinProperties);
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> void batchPersistJoinCollection(PersistenceContext<ID> context,
			PropertyMeta<Void, V> propertyMeta, Collection<V> joinCollection,
			EntityPersister persister)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) joinProperties.getEntityMeta();
		PropertyMeta<Void, JOIN_ID> joinIdMeta = joinEntityMeta.getIdMeta();
		int count = 0;
		for (V joinEntity : joinCollection)
		{
			DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
					propertyMeta, count);

			Object joinEntityId = introspector
					.getValueFromField(joinEntity, joinIdMeta.getGetter());

			String joinEntityIdStringValue = joinIdMeta.writeValueToString(joinEntityId);
			if (joinEntityIdStringValue != null)
			{
				context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
						joinEntityIdStringValue,
						context.getEntityMutator(context.getColumnFamilyName()));

				PersistenceContext<JOIN_ID> joinPersistenceContext = (PersistenceContext<JOIN_ID>) context
						.newPersistenceContext(propertyMeta.joinMeta(),
								proxifier.unproxy(joinEntity));

				persister.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity,
						joinProperties);
			}
			count++;
		}
	}

	@SuppressWarnings("unchecked")
	public <ID, K, V, JOIN_ID> void batchPersistJoinMap(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, Map<K, V> joinMap, EntityPersister persiter)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) joinProperties.getEntityMeta();
		PropertyMeta<Void, JOIN_ID> idMeta = joinEntityMeta.getIdMeta();

		for (Entry<K, V> entry : joinMap.entrySet())
		{
			DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
					propertyMeta, entry.getKey().hashCode());

			V joinEntity = entry.getValue();
			Object joinEntityId = introspector.getValueFromField(joinEntity, idMeta.getGetter());
			String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);

			String value = propertyMeta.writeValueToString(new KeyValue<K, String>(entry.getKey(),
					joinEntityIdStringValue));
			context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
					context.getEntityMutator(context.getColumnFamilyName()));

			PersistenceContext<JOIN_ID> joinPersistenceContext = (PersistenceContext<JOIN_ID>) context
					.newPersistenceContext(propertyMeta.joinMeta(), proxifier.unproxy(joinEntity));

			persiter.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity,
					joinProperties);
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
						removeCounterWideMap(context, (PropertyMeta<Void, Long>) propertyMeta);
					}
					else
					{
						removeSimpleCounter(context, (PropertyMeta<Void, Long>) propertyMeta);
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
