package fr.doan.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;

import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.EntityMapper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.validation.Validator;

public class EntityLoader
{

	private EntityMapper mapper = new EntityMapper();

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	public <T, ID> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entityClass, "entity class");
		Validator.validateNotNull(key, "entity key");
		Validator.validateNotNull(entityMeta, "entity meta");

		T entity = null;
		try
		{

			List<Pair<DynamicComposite, Object>> columns = entityMeta.getEntityDao()
					.eagerFetchEntity(key);

			if (columns.size() > 0)
			{
				entity = entityClass.newInstance();
				mapper.mapColumnsToBean(key, columns, entityMeta, entity);
			}

		}
		catch (Exception e)
		{
			throw new RuntimeException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + key + "'", e);
		}
		return entity;
	}

	public <ID, V> V loadSimpleProperty(ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite composite = keyFactory.createBaseForInsert(propertyMeta, 0);
		Object value = dao.getValue(key, composite);

		return propertyMeta.getValue(value);
	}

	public <ID, V> List<V> loadListProperty(ID key, GenericEntityDao<ID> dao,
			ListMeta<V> listPropertyMeta)
	{
		DynamicComposite start = keyFactory.createBaseForQuery(listPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(listPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		List<V> list = listPropertyMeta.newListInstance();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			list.add(listPropertyMeta.getValue(pair.right));
		}
		return list;
	}

	public <ID, V> Set<V> loadSetProperty(ID key, GenericEntityDao<ID> dao,
			SetMeta<V> setPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(setPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(setPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Set<V> set = setPropertyMeta.newSetInstance();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			set.add(setPropertyMeta.getValue(pair.right));
		}
		return set;
	}

	public <ID, K, V> Map<K, V> loadMapProperty(ID key, GenericEntityDao<ID> dao,
			MapMeta<K, V> mapPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(mapPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(mapPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Map<K, V> map = mapPropertyMeta.newMapInstance();

		Class<K> keyClass = mapPropertyMeta.getKeyClass();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			KeyValueHolder holder = (KeyValueHolder) pair.right;

			map.put(keyClass.cast(holder.getKey()), mapPropertyMeta.getValue(holder.getValue()));
		}
		return map;
	}

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		Object value = null;
		switch (propertyMeta.propertyType())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				value = this.loadSimpleProperty(key, dao, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				value = this.loadListProperty(key, dao, (ListMeta<?>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = this.loadSetProperty(key, dao, (SetMeta<?>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = this.loadMapProperty(key, dao, (MapMeta<?, ?>) propertyMeta);
				break;
			default:
				break;
		}
		try
		{
			propertyMeta.getSetter().invoke(realObject, value);
		}
		catch (Exception e)
		{}
	}
}
