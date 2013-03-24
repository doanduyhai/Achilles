package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentWriteConsistencyLevel;
import static info.archinnov.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
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

	private EntityHelper helper = new EntityHelper();
	private EntityLoader loader = new EntityLoader();

	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory = new DynamicCompositeKeyFactory();
	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	public <ID> void persist(PersistenceContext<ID> context, Mutator<ID> mutator)
	{
		EntityMeta<ID> entityMeta = context.getEntityMeta();

		if (!entityMeta.isColumnFamilyDirectMapping())
		{
			this.batchPersistVersionSerialUID(context, mutator);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				this.persistProperty(context, propertyMeta, mutator);
			}
		}
	}

	public <ID> void persist(PersistenceContext<ID> context)
	{
		GenericDynamicCompositeDao<ID> dao = context.fetchEntityDao();
		if (dao != null)
		{
			Mutator<ID> mutator = dao.buildMutator();
			this.persist(context, mutator);
			dao.executeMutator(mutator);
		}
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta, Mutator<ID> mutator)
	{

		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.batchPersistSimpleProperty(context, propertyMeta, mutator);
				break;
			case COUNTER:
				this.atomicPersistCounter(context, (PropertyMeta<Void, Long>) propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.batchPersistListProperty(context, propertyMeta, mutator);
				break;
			case SET:
			case LAZY_SET:
				this.batchPersistSetProperty(context, propertyMeta, mutator);
				break;
			case MAP:
			case LAZY_MAP:
				this.batchPersistMapProperty(context, propertyMeta, mutator);
				break;
			case JOIN_SIMPLE:
				this.batchPersistJoinEntity(context, propertyMeta, mutator);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				this.batchPersistJoinListOrSetProperty(context, propertyMeta, mutator);
				break;
			case JOIN_MAP:
				this.batchPersistJoinMapProperty(context, propertyMeta, mutator);
				break;
			default:
				break;
		}
	}

	protected <ID> void batchPersistSimpleProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		DynamicComposite name = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		String value = propertyMeta.writeValueToString(helper.getValueFromField(
				context.getEntity(), propertyMeta.getGetter()));
		if (value != null)
		{
			context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
					mutator);
		}
	}

	protected <ID> void batchPersistListProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		List<?> list = (List<?>) helper.getValueFromField(context.getEntity(),
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
					context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							stringValue, mutator);
				}
				count++;
			}
		}
	}

	protected <ID> void batchPersistSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) helper.getValueFromField(context.getEntity(),
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
					context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							stringValue, mutator);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <ID, K, V> void batchPersistMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, Mutator<ID> mutator)
	{

		Map<K, V> map = (Map<K, V>) helper.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (map != null)
		{
			for (Entry<K, V> entry : map.entrySet())
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, entry.getKey().hashCode());

				String value = propertyMeta.writeValueToString(new KeyValue<K, V>(entry.getKey(),
						entry.getValue()));
				context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
						mutator);
			}
		}
	}

	public <ID> void batchPersistJoinEntity(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		PropertyMeta<Void, ?> idMeta = propertyMeta.joinIdMeta();
		Object joinEntity = helper.getValueFromField(context.getEntity(), propertyMeta.getGetter());

		if (joinEntity != null)
		{
			Object joinId = helper.getKey(joinEntity, idMeta);
			Validator.validateNotNull(joinId, "Primary key for join entity '" + joinEntity
					+ "' should not be null");
			String joinIdString = idMeta.writeValueToString(joinId);

			DynamicComposite joinName = dynamicCompositeKeyFactory
					.createForBatchInsertSingleValue(propertyMeta);
			context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), joinName,
					joinIdString, mutator);

			cascadePersistOrEnsureExists(context, joinEntity, joinProperties);
		}
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(PersistenceContext<ID> context,
			V joinEntity, JoinProperties joinProperties, Mutator<JOIN_ID> joinMutator)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) joinProperties.getEntityMeta();
		JOIN_ID joinId = helper.getKey(joinEntity, joinMeta.getIdMeta());
		Validate.notNull(joinId, "key value for entity '" + joinMeta.getClassName()
				+ "' should not be null");

		Set<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(ALL) || cascadeTypes.contains(PERSIST))
		{
			PersistenceContext<JOIN_ID> joinPersistenceContext = context.newPersistenceContext(
					joinMeta, helper.unproxy(joinEntity));
			persist(joinPersistenceContext, joinMutator);
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

	@SuppressWarnings("unchecked")
	protected <ID, K, V, JOIN_ID> void batchPersistJoinMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, Mutator<ID> mutator)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = context.findEntityDao(joinEntityMeta
				.getColumnFamilyName());
		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();

		Map<K, V> map = (Map<K, V>) helper.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		if (map != null)
		{
			for (Entry<K, V> entry : map.entrySet())
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, entry.getKey().hashCode());

				V joinEntity = entry.getValue();
				Object joinEntityId = helper.getValueFromField(joinEntity, idMeta.getGetter());
				String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);

				String value = propertyMeta.writeValueToString(new KeyValue<K, String>(entry
						.getKey(), joinEntityIdStringValue));
				context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
						mutator);
				cascadePersistOrEnsureExists(context, joinEntity, joinProperties, joinMutator);
			}
		}

		joinEntityDao.executeMutator(joinMutator);
	}

	@SuppressWarnings("unchecked")
	public <ID> void remove(PersistenceContext<ID> context)
	{
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		if (context.isDirectColumnFamilyMapping())
		{
			context.fetchColumnFamilyDao().removeRow(primaryKey);
		}
		else
		{

			context.fetchEntityDao().removeRow(primaryKey);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				if (propertyMeta.isExternal())
				{
					ExternalWideMapProperties<ID> externalProperties = (ExternalWideMapProperties<ID>) propertyMeta
							.getExternalWideMapProperties();
					context.findColumnFamilyDao(externalProperties.getExternalColumnFamilyName())
							.removeRow(primaryKey);
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

	public <ID, V> void removePropertyBatch(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta, Mutator<ID> mutator)
	{
		Validate.notNull(key, "key should not be null");
		DynamicComposite start = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				ComponentEquality.EQUAL);
		DynamicComposite end = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				GREATER_THAN_EQUAL);
		dao.removeColumnRangeBatch(key, start, end, mutator);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(PersistenceContext<ID> context,
			V joinEntity, JoinProperties joinProperties)
	{

		String joinColumnFamilyName = joinProperties.getEntityMeta().getColumnFamilyName();
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = (GenericDynamicCompositeDao<JOIN_ID>) context
				.findEntityDao(joinColumnFamilyName);
		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();
		JOIN_ID joinId = cascadePersistOrEnsureExists(context, joinEntity, joinProperties,
				joinMutator);
		joinEntityDao.executeMutator(joinMutator);

		return joinId;
	}

	@SuppressWarnings("unchecked")
	private <ID, JOIN_ID> void batchPersistJoinListOrSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = (GenericDynamicCompositeDao<JOIN_ID>) context
				.findEntityDao(joinEntityMeta.getColumnFamilyName());

		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();

		Collection<?> list = (Collection<?>) helper.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());
		int count = 0;
		if (list != null)
		{
			for (Object joinEntity : list)
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, count);

				Object joinEntityId = helper.getValueFromField(joinEntity, idMeta.getGetter());

				String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);
				if (joinEntityIdStringValue != null)
				{
					context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
							joinEntityIdStringValue, mutator);
					this.cascadePersistOrEnsureExists(context, joinEntity, joinProperties,
							joinMutator);
				}
				count++;
			}
		}
		joinEntityDao.executeMutator(joinMutator);
	}

	private <T, ID> void batchPersistVersionSerialUID(PersistenceContext<ID> context,
			Mutator<ID> mutator)
	{

		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, PropertyType.SERIAL_VERSION_UID.name(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		Long serialVersionUID = helper.findSerialVersionUID(context.getEntityClass());

		if (serialVersionUID != null)
		{
			context.fetchEntityDao().insertColumnBatch(context.getPrimaryKey(), composite,
					serialVersionUID.toString(), mutator);
		}
		else
		{
			throw new AchillesException("Cannot find 'serialVersionUID' field for entity class '"
					+ context.getEntityClass().getCanonicalName() + "'");
		}
	}

	@SuppressWarnings("unchecked")
	private <ID> void atomicPersistCounter(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta)
	{
		CounterDao dao = context.getCounterDao();
		String fqcn = propertyMeta.fqcn();
		PropertyMeta<Void, ID> idMeta = (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta();

		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, context.getPrimaryKey(),
				idMeta);
		DynamicComposite comp = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		Object counterValue = helper.getValueFromField(context.getEntity(),
				propertyMeta.getGetter());

		if (counterValue != null)
		{
			insertCounterWithConsistencyLevel(propertyMeta, dao, keyComp, comp, counterValue);
		}
	}

	private void insertCounterWithConsistencyLevel(PropertyMeta<Void, Long> propertyMeta,
			CounterDao dao, Composite keyComp, DynamicComposite comp, Object counterValue)
	{
		boolean resetConsistencyLevel = false;
		if (currentWriteConsistencyLevel.get() == null)
		{
			currentWriteConsistencyLevel.set(propertyMeta.getWriteConsistencyLevel());
			resetConsistencyLevel = true;
		}
		try
		{
			dao.insertCounter(keyComp, comp, (Long) counterValue);
		}
		catch (Throwable throwable)
		{
			throw new RuntimeException(throwable);
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				currentWriteConsistencyLevel.remove();
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

		context.getCounterDao().removeCounter(keyComp, com);
	}

	@SuppressWarnings("unchecked")
	private <ID> void removeCounterWideMap(PersistenceContext<ID> context,
			PropertyMeta<Void, Long> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(),
				context.getPrimaryKey(), (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		context.getCounterDao().removeCounterRow(keyComp);
	}
}
