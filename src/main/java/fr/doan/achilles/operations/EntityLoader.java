package fr.doan.achilles.operations;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.bean.BeanMapper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.validation.Validator;

public class EntityLoader
{

	private BeanMapper mapper = new BeanMapper();

	public <T extends Object, ID extends Serializable> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entityClass, "entity class");
		Validator.validateNotNull(key, "entity key");
		Validator.validateNotNull(entityMeta, "entity meta");

		T entity = null;
		try
		{

			List<Pair<Composite, Object>> columns = entityMeta.getDao().eagerFetchEntity(key);

			if (columns.size() > 0)
			{
				entity = entityClass.newInstance();
				mapper.mapColumnsToBean(key, columns, entityMeta, entity);
			}

		}
		catch (Exception e)
		{
			throw new RuntimeException("Error when loading entity type '" + entityClass.getCanonicalName() + "' with key '" + key + "'", e);
		}
		return entity;
	}

	public <ID extends Serializable, V extends Serializable> V loadSimpleProperty(ID key, String propertyName, GenericDao<ID> dao,
			PropertyMeta<V> propertyMeta)
	{
		Composite composite = dao.buildCompositeForProperty(propertyName, PropertyType.SIMPLE, 0);
		Object value = dao.getValue(key, composite);

		return propertyMeta.get(value);
	}

	public <ID extends Serializable, V extends Serializable> List<V> loadListProperty(ID key, String propertyName, GenericDao<ID> dao,
			ListPropertyMeta<V> listpropertyMeta)
	{
		Composite start = dao.buildCompositeComparatorStart(propertyName, PropertyType.LIST);
		Composite end = dao.buildCompositeComparatorEnd(propertyName, PropertyType.LIST);
		List<Pair<Composite, Object>> columns = dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
		List<V> list = listpropertyMeta.newListInstance();
		for (Pair<Composite, Object> pair : columns)
		{
			list.add(listpropertyMeta.get(pair.right));
		}
		return list;
	}

	public <ID extends Serializable, V extends Serializable> Set<V> loadSetProperty(ID key, String propertyName, GenericDao<ID> dao,
			SetPropertyMeta<V> setPropertyMeta)
	{

		Composite start = dao.buildCompositeComparatorStart(propertyName, PropertyType.SET);
		Composite end = dao.buildCompositeComparatorEnd(propertyName, PropertyType.SET);
		List<Pair<Composite, Object>> columns = dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
		Set<V> set = setPropertyMeta.newSetInstance();
		for (Pair<Composite, Object> pair : columns)
		{
			set.add(setPropertyMeta.get(pair.right));
		}
		return set;
	}

	@SuppressWarnings("unchecked")
	public <ID extends Serializable, K extends Serializable, V extends Serializable> Map<K, V> loadMapProperty(ID key, String propertyName,
			GenericDao<ID> dao, MapPropertyMeta<V> mapPropertyMeta)
	{

		Composite start = dao.buildCompositeComparatorStart(propertyName, PropertyType.MAP);
		Composite end = dao.buildCompositeComparatorEnd(propertyName, PropertyType.MAP);
		List<Pair<Composite, Object>> columns = dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
		Map<K, V> map = mapPropertyMeta.newMapInstance();

		Class<K> keyClass = mapPropertyMeta.getKeyClass();
		for (Pair<Composite, Object> pair : columns)
		{
			KeyValueHolder holder = (KeyValueHolder) pair.right;

			map.put(keyClass.cast(holder.getKey()), mapPropertyMeta.get(holder.getValue()));
		}
		return map;
	}
}
