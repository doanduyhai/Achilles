package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentWriteConsistencyLevel;
import static info.archinnov.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.BeanMappingException;
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

	// static ThreadLocal<Mutator<Composite>> counterMutatorTL = new ThreadLocal<Mutator<Composite>>();

	public <ID> void persist(Object entity, EntityMeta<ID> entityMeta, Mutator<ID> mutator)
	{
		if (!entityMeta.isColumnFamilyDirectMapping())
		{
			ID key = helper.getKey(entity, entityMeta.getIdMeta());
			Validate.notNull(key, "key value for entity '" + entityMeta.getClassName()
					+ "' should not be null");
			GenericDynamicCompositeDao<ID> dao = entityMeta.getEntityDao();
			this.batchPersistVersionSerialUID(entity.getClass(), key, dao, mutator);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				this.persistProperty(entity, key, dao, propertyMeta, mutator);
			}
		}
	}

	public <ID> void persist(Object entity, EntityMeta<ID> entityMeta)
	{
		GenericDynamicCompositeDao<ID> dao = entityMeta.getEntityDao();
		if (dao != null)
		{
			Mutator<ID> mutator = dao.buildMutator();
			this.persist(entity, entityMeta, mutator);
			dao.executeMutator(mutator);
		}
	}

	public <ID> void batchPersistJoinEntity(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		PropertyMeta<Void, ?> idMeta = propertyMeta.joinIdMeta();
		Object joinEntity = helper.getValueFromField(entity, propertyMeta.getGetter());

		if (joinEntity != null)
		{
			String joinId = idMeta.writeValueToString(this.cascadePersistOrEnsureExists(joinEntity,
					joinProperties));

			if (joinId != null)
			{
				DynamicComposite joinName = dynamicCompositeKeyFactory
						.createForBatchInsertSingleValue(propertyMeta);
				dao.insertColumnBatch(key, joinName, joinId, mutator);

				cascadePersistOrEnsureExists(joinEntity, joinProperties);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(V joinEntity,
			JoinProperties joinProperties)
	{
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = (GenericDynamicCompositeDao<JOIN_ID>) joinProperties
				.getEntityMeta().getEntityDao();
		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();
		JOIN_ID joinId = this.cascadePersistOrEnsureExists(joinEntity, joinProperties, joinMutator);
		joinEntityDao.executeMutator(joinMutator);

		return joinId;
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, ID, V> JOIN_ID cascadePersistOrEnsureExists(V joinEntity,
			JoinProperties joinProperties, Mutator<JOIN_ID> joinMutator)
	{
		EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) joinProperties.getEntityMeta();
		JOIN_ID joinId = helper.getKey(joinEntity, joinEntityMeta.getIdMeta());
		Validate.notNull(joinId, "key value for entity '" + joinEntityMeta.getClassName()
				+ "' should not be null");

		List<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(PERSIST) || cascadeTypes.contains(ALL))
		{
			this.persist(helper.unproxy(joinEntity), joinEntityMeta, joinMutator);
		}
		else
		{
			Long joinVersionSerialUID = loader.loadVersionSerialUID(joinId,
					joinEntityMeta.getEntityDao());
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

	protected <ID> void batchPersistSimpleProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		DynamicComposite name = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		String value = propertyMeta.writeValueToString(helper.getValueFromField(entity,
				propertyMeta.getGetter()));
		if (value != null)
		{
			dao.insertColumnBatch(key, name, value, mutator);
		}
	}

	protected <ID> void batchPersistSetProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) helper.getValueFromField(entity, propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, value.hashCode());

				String stringValue = propertyMeta.writeValueToString(value);
				if (stringValue != null)
				{
					dao.insertColumnBatch(key, name, stringValue, mutator);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <ID, K, V> void batchPersistMapProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> propertyMeta, Mutator<ID> mutator)
	{

		Map<K, V> map = (Map<K, V>) helper.getValueFromField(entity, propertyMeta.getGetter());
		if (map != null)
		{
			for (Entry<K, V> entry : map.entrySet())
			{
				DynamicComposite name = dynamicCompositeKeyFactory.createForBatchInsertMultiValue(
						propertyMeta, entry.getKey().hashCode());

				String value = propertyMeta.writeValueToString(new KeyValue<K, V>(entry.getKey(),
						entry.getValue()));
				dao.insertColumnBatch(key, name, value, mutator);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <ID, K, V, JOIN_ID> void batchPersistJoinMapProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> propertyMeta, Mutator<ID> mutator)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = (GenericDynamicCompositeDao<JOIN_ID>) joinEntityMeta
				.getEntityDao();
		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();

		Map<K, V> map = (Map<K, V>) helper.getValueFromField(entity, propertyMeta.getGetter());
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
				dao.insertColumnBatch(key, name, value, mutator);
				this.cascadePersistOrEnsureExists(joinEntity, joinProperties, joinMutator);
			}
		}

		joinEntityDao.executeMutator(joinMutator);
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta, Mutator<ID> mutator)
	{

		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.batchPersistSimpleProperty(entity, key, dao, propertyMeta, mutator);
				break;
			case COUNTER:
				this.atomicPersistCounter(entity, key, (PropertyMeta<Void, Long>) propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.batchPersistListProperty(entity, key, dao, propertyMeta, mutator);
				break;
			case SET:
			case LAZY_SET:
				this.batchPersistSetProperty(entity, key, dao, propertyMeta, mutator);
				break;
			case MAP:
			case LAZY_MAP:
				this.batchPersistMapProperty(entity, key, dao, propertyMeta, mutator);
				break;
			case JOIN_SIMPLE:
				this.batchPersistJoinEntity(entity, key, dao, propertyMeta, mutator);
				break;
			case JOIN_LIST:
			case JOIN_SET:
				this.batchPersistJoinListOrSetProperty(entity, key, dao, propertyMeta, mutator);
				break;
			case JOIN_MAP:
				this.batchPersistJoinMapProperty(entity, key, dao, propertyMeta, mutator);
				break;
			default:
				break;
		}
	}

	public <ID> void remove(Object entity, EntityMeta<ID> entityMeta)
	{
		ID key = helper.getKey(entity, entityMeta.getIdMeta());
		removeById(key, entityMeta);
	}

	@SuppressWarnings("unchecked")
	public <ID> void removeById(ID id, EntityMeta<ID> entityMeta)
	{
		Validate.notNull(id, "key value for entity '" + entityMeta.getClassName() + "'");

		AbstractDao<ID, ?, ?> dao;
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			dao = entityMeta.getColumnFamilyDao();
		}
		else
		{
			dao = entityMeta.getEntityDao();
		}
		dao.removeRow(id);
		for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
		{
			PropertyMeta<?, ?> propertyMeta = entry.getValue();
			if (propertyMeta.isExternal())
			{
				ExternalWideMapProperties<ID> externalWideMapProperties = (ExternalWideMapProperties<ID>) propertyMeta
						.getExternalWideMapProperties();
				externalWideMapProperties.getExternalWideMapDao().removeRow(id);
			}

			if (propertyMeta.isCounter())
			{
				if (propertyMeta.isWideMap())
				{
					this.removeCounterWideMap(id, (PropertyMeta<Void, Long>) propertyMeta);
				}
				else
				{
					this.removeSimpleCounter(id, (PropertyMeta<Void, Long>) propertyMeta);
				}
			}
		}
	}

	public <ID, V> void removeProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		Validate.notNull(key, "key should not be null");
		DynamicComposite start = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				ComponentEquality.EQUAL);
		DynamicComposite end = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				GREATER_THAN_EQUAL);
		dao.removeColumnRange(key, start, end);
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

	private <ID> void batchPersistListProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		List<?> list = (List<?>) helper.getValueFromField(entity, propertyMeta.getGetter());
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
					dao.insertColumnBatch(key, name, stringValue, mutator);
				}
				count++;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <ID, JOIN_ID> void batchPersistJoinListOrSetProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		EntityMeta<?> joinEntityMeta = joinProperties.getEntityMeta();
		PropertyMeta<Void, ?> idMeta = joinEntityMeta.getIdMeta();
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = (GenericDynamicCompositeDao<JOIN_ID>) joinEntityMeta
				.getEntityDao();
		Mutator<JOIN_ID> joinMutator = joinEntityDao.buildMutator();

		Collection<?> list = (Collection<?>) helper.getValueFromField(entity,
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
					dao.insertColumnBatch(key, name, joinEntityIdStringValue, mutator);
					this.cascadePersistOrEnsureExists(joinEntity, joinProperties, joinMutator);
				}
				count++;
			}
		}

		joinEntityDao.executeMutator(joinMutator);
	}

	private <T, ID> void batchPersistVersionSerialUID(Class<T> entityClass, ID key,
			GenericDynamicCompositeDao<ID> dao, Mutator<ID> mutator)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, PropertyType.SERIAL_VERSION_UID.name(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		if (serialVersionUID != null)
		{
			dao.insertColumnBatch(key, composite, serialVersionUID.toString(), mutator);
		}
		else
		{
			throw new BeanMappingException(
					"Cannot find 'serialVersionUID' field for entity class '"
							+ entityClass.getCanonicalName() + "'");
		}
	}

	@SuppressWarnings("unchecked")
	private <ID> void atomicPersistCounter(Object entity, ID key,
			PropertyMeta<Void, Long> propertyMeta)
	{
		CounterDao dao = propertyMeta.counterDao();
		String fqcn = propertyMeta.fqcn();
		PropertyMeta<Void, ID> idMeta = (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta();

		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, key, idMeta);
		DynamicComposite comp = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);
		Object counterValue = helper.getValueFromField(entity, propertyMeta.getGetter());

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
	private <ID> void removeSimpleCounter(ID key, PropertyMeta<Void, Long> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(), key,
				(PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		DynamicComposite com = dynamicCompositeKeyFactory
				.createForBatchInsertSingleValue(propertyMeta);

		propertyMeta.counterDao().removeCounter(keyComp, com);
	}

	@SuppressWarnings("unchecked")
	private <ID> void removeCounterWideMap(ID key, PropertyMeta<Void, Long> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(), key,
				(PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		propertyMeta.counterDao().removeCounterRow(keyComp);
	}
}
