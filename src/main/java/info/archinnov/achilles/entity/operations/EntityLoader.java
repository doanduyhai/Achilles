package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentReadConsistencyLevel;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.commons.lang.StringUtils;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader
{

	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();
	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory = new DynamicCompositeKeyFactory();
	private EntityMapper mapper = new EntityMapper();
	private EntityHelper helper = new EntityHelper();
	private JoinEntityLoader joinLoader = new JoinEntityLoader();

	@SuppressWarnings("unchecked")
	public <T, ID> T load(PersistenceContext<ID> context)
	{
		Class<T> entityClass = (Class<T>) context.getEntityClass();
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '" + entityClass.getCanonicalName()
				+ "' key should not be null");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		T entity = null;
		try
		{

			if (entityMeta.isColumnFamilyDirectMapping())
			{
				entity = entityClass.newInstance();
				helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
			}
			else
			{
				List<Pair<DynamicComposite, String>> columns = context.getEntityDao()
						.eagerFetchEntity(primaryKey);
				if (columns.size() > 0)
				{
					entity = entityClass.newInstance();
					mapper.setEagerPropertiesToEntity(primaryKey, columns, entityMeta, entity);
					helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
				}
			}

		}
		catch (Exception e)
		{
			throw new AchillesException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + primaryKey + "'", e);
		}
		return entity;
	}

	protected <ID, V> Long loadVersionSerialUID(ID key, GenericDynamicCompositeDao<ID> dao)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, PropertyType.SERIAL_VERSION_UID.name(), ComponentEquality.EQUAL);

		String serialVersionUIDString = dao.getValue(key, composite);
		if (StringUtils.isNotBlank(serialVersionUIDString))
		{
			return Long.parseLong(serialVersionUIDString);
		}
		else
		{
			return null;
		}
	}

	protected <ID, V> V loadSimpleProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite composite = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				EQUAL);
		return propertyMeta.getValueFromString(context.getEntityDao().getValue(
				context.getPrimaryKey(), composite));
	}

	@SuppressWarnings("unchecked")
	protected <ID> Long loadSimpleCounterProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(propertyMeta.fqcn(),
				context.getPrimaryKey(), (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		DynamicComposite comp = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta, EQUAL);

		Long counter = loadCounterWithConsistencyLevel(propertyMeta, keyComp, comp,
				context.getCounterDao());

		return counter;
	}

	private Long loadCounterWithConsistencyLevel(PropertyMeta<?, ?> propertyMeta,
			Composite keyComp, DynamicComposite comp, CounterDao counterDao)
	{
		boolean resetConsistencyLevel = false;
		if (currentReadConsistencyLevel.get() == null)
		{
			currentReadConsistencyLevel.set(propertyMeta.getReadConsistencyLevel());
			resetConsistencyLevel = true;
		}
		Long counter;
		try
		{
			counter = counterDao.getCounterValue(keyComp, comp);
		}
		catch (Throwable throwable)
		{
			throw new RuntimeException(throwable);
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				currentReadConsistencyLevel.remove();
			}
		}
		return counter;
	}

	protected <ID, V> List<V> loadListProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		List<Pair<DynamicComposite, String>> columns = fetchColumns(context, propertyMeta);
		List<V> list = null;
		if (columns.size() > 0)
		{
			list = propertyMeta.newListInstance();
			for (Pair<DynamicComposite, String> pair : columns)
			{
				list.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return list;
	}

	protected <ID, V> Set<V> loadSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{

		List<Pair<DynamicComposite, String>> columns = fetchColumns(context, propertyMeta);
		Set<V> set = null;
		if (columns.size() > 0)
		{
			set = propertyMeta.newSetInstance();
			for (Pair<DynamicComposite, String> pair : columns)
			{
				set.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return set;
	}

	protected <ID, K, V> Map<K, V> loadMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{
		List<Pair<DynamicComposite, String>> columns = fetchColumns(context, propertyMeta);
		Class<K> keyClass = propertyMeta.getKeyClass();
		Map<K, V> map = null;
		if (columns.size() > 0)
		{
			map = propertyMeta.newMapInstance();
			for (Pair<DynamicComposite, String> pair : columns)
			{
				KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

				map.put(keyClass.cast(holder.getKey()),
						propertyMeta.getValueFromString(holder.getValue()));
			}
		}
		return map;
	}

	private <ID, V> List<Pair<DynamicComposite, String>> fetchColumns(
			PersistenceContext<ID> context, PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite start = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta, EQUAL);
		DynamicComposite end = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);
		return columns;
	}

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key,
			PersistenceContext<ID> context, PropertyMeta<?, V> propertyMeta)
	{
		Object value = null;
		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				value = loadSimpleProperty(context, propertyMeta);
				break;
			case COUNTER:
				value = loadSimpleCounterProperty(context, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				value = loadListProperty(context, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = loadSetProperty(context, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = loadMapProperty(context, propertyMeta);
				break;
			case JOIN_SIMPLE:
				value = loadJoinSimple(context, propertyMeta);
				break;
			case JOIN_LIST:
				value = joinLoader.loadJoinListProperty(context, propertyMeta);
				break;
			case JOIN_SET:
				value = joinLoader.loadJoinSetProperty(context, propertyMeta);
				break;
			case JOIN_MAP:
				value = joinLoader.loadJoinMapProperty(context, propertyMeta);
				break;
			default:
				return;
		}
		helper.setValueToField(realObject, propertyMeta.getSetter(), value);
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> V loadJoinSimple(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

		DynamicComposite composite = dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta,
				EQUAL);

		String stringJoinId = context.getEntityDao().getValue(context.getPrimaryKey(), composite);

		if (stringJoinId != null)
		{
			JOIN_ID joinId = joinIdMeta.getValueFromString(stringJoinId);
			PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
					propertyMeta.getValueClass(), joinMeta, joinId);
			return this.load(joinContext);

		}
		else
		{
			return null;
		}
	}

}
