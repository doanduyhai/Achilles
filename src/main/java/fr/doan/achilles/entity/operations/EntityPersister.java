package fr.doan.achilles.entity.operations;

import static fr.doan.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.CascadeType;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.Validate;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.AbstractDao;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.validation.Validator;

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

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	public <ID> void persist(Object entity, EntityMeta<ID> entityMeta)
	{
		if (!entityMeta.isWideRow())
		{
			ID key = helper.getKey(entity, entityMeta.getIdMeta());
			Validate.notNull(key, "key value for entity '" + entityMeta.getClassName()
					+ "' should not be null");
			GenericDynamicCompositeDao<ID> dao = entityMeta.getEntityDao();

			Mutator<ID> mutator = dao.buildMutator();

			this.batchPersistVersionSerialUID(entity.getClass(), key, dao, mutator);
			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				switch (propertyMeta.type())
				{
					case SIMPLE:
					case LAZY_SIMPLE:
						this.batchPersistSimpleProperty(entity, key, dao, propertyMeta, mutator);
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
					default:
						break;
				}
			}
			mutator.execute();
		}

	}

	private <T, ID> void batchPersistVersionSerialUID(Class<T> entityClass, ID key,
			GenericDynamicCompositeDao<ID> dao, Mutator<ID> mutator)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, PropertyType.SERIAL_VERSION_UID.name(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(2, 0, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		if (serialVersionUID != null)
		{
			dao.insertColumn(key, composite, serialVersionUID, mutator);
		}
		else
		{
			throw new BeanMappingException(
					"Cannot find 'serialVersionUID' field for entity class '"
							+ entityClass.getCanonicalName() + "'");
		}
	}

	private <ID> void batchPersistSimpleProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, 0);
		Object value = helper.getValueFromField(entity, propertyMeta.getGetter());
		if (value != null)
		{
			dao.insertColumn(key, name, value, mutator);
		}
	}

	public <ID> void batchPersistJoinEntity(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Object joinEntity = null;
		joinEntity = helper.getValueFromField(entity, propertyMeta.getGetter());

		if (joinEntity != null)
		{
			Object joinId = this.cascadePersistOrEnsureExists(joinEntity, joinProperties);

			DynamicComposite joinName = keyFactory.createForBatchInsert(propertyMeta, 0);
			dao.insertColumn(key, joinName, joinId, mutator);
		}
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, V> JOIN_ID cascadePersistOrEnsureExists(V joinEntity,
			JoinProperties joinProperties)
	{
		EntityMeta<JOIN_ID> joinEntityMeta = joinProperties.getEntityMeta();
		JOIN_ID joinId = helper.getKey(joinEntity, joinEntityMeta.getIdMeta());
		Validate.notNull(joinId, "key value for entity '" + joinEntityMeta.getClassName()
				+ "' should not be null");

		List<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(PERSIST) || cascadeTypes.contains(ALL))
		{
			this.persist(joinEntity, joinEntityMeta);
		}
		else
		{
			Object loadedEntity = loader.load(joinEntity.getClass(), joinId, joinEntityMeta);
			Validator
					.validateNotNull(
							loadedEntity,
							"The entity '"
									+ joinProperties.getEntityMeta().getClassName()
									+ "' with id '"
									+ joinId
									+ "' cannot be found. Maybe you should persist it first or set enable CascadeType.PERSIST");
		}

		return joinId;

	}

	public <ID> void persistSimpleProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta)
	{
		this.batchPersistSimpleProperty(entity, key, dao, propertyMeta, null);
	}

	public <ID> void persistJoinEntity(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		this.batchPersistJoinEntity(entity, key, dao, propertyMeta, null);
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
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, count);
				if (value != null)
				{
					dao.insertColumn(key, name, value, mutator);
				}
				count++;
			}
		}
	}

	public <ID> void persistListProperty(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchPersistListProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID> void batchPersistSetProperty(Object entity, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) helper.getValueFromField(entity, propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta,
						value.hashCode());
				if (value != null)
				{
					dao.insertColumn(key, name, value, mutator);
				}
			}
		}
	}

	public <ID> void persistSetProperty(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchPersistSetProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
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
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, entry
						.getKey().hashCode());

				KeyValueHolder<K, V> value = new KeyValueHolder<K, V>(entry.getKey(),
						entry.getValue());
				dao.insertColumn(key, name, value, mutator);
			}
		}
	}

	public <ID> void persistMapProperty(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchPersistMapProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	public <ID, V> void persistProperty(Object entity, ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{

		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.persistSimpleProperty(entity, key, dao, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.persistListProperty(entity, key, dao, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				this.persistSetProperty(entity, key, dao, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				this.persistMapProperty(entity, key, dao, propertyMeta);
				break;
			case JOIN_SIMPLE:
				this.persistJoinEntity(entity, key, dao, propertyMeta);
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
		if (entityMeta.isWideRow())
		{
			dao = entityMeta.getEntityDao();
		}
		else
		{
			dao = entityMeta.getEntityDao();
		}
		dao.removeRow(id);
		for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
		{
			ExternalWideMapProperties<ID> externalWideMapProperties = (ExternalWideMapProperties<ID>) entry
					.getValue().getExternalWideMapProperties();
			if (externalWideMapProperties != null)
			{
				externalWideMapProperties.getExternalWideMapDao().removeRow(id);
			}
		}
	}

	public <ID, V> void removeProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		Validate.notNull(key, "key value");
		DynamicComposite start = keyFactory.createBaseForQuery(propertyMeta,
				ComponentEquality.EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		dao.removeColumnRange(key, start, end);
	}
}
