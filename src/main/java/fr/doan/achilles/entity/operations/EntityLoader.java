package fr.doan.achilles.entity.operations;

import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.EntityMapper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetPropertyMeta;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.validation.Validator;

public class EntityLoader
{

	private EntityMapper mapper = new EntityMapper();

	public <T, ID> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta)
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

	public <ID, V> V loadSimpleProperty(ID key, GenericDao<ID> dao, PropertyMeta<V> propertyMeta)
	{
		Composite composite = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), 0);
		Object value = dao.getValue(key, composite);

		return propertyMeta.get(value);
	}

	public <ID, V> List<V> loadListProperty(ID key, GenericDao<ID> dao, ListPropertyMeta<V> listPropertyMeta)
	{
		Composite start = dao.buildCompositeComparatorStart(listPropertyMeta.getPropertyName(), listPropertyMeta.propertyType());
		Composite end = dao.buildCompositeComparatorEnd(listPropertyMeta.getPropertyName(), listPropertyMeta.propertyType());
		List<Pair<Composite, Object>> columns = dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
		List<V> list = listPropertyMeta.newListInstance();
		for (Pair<Composite, Object> pair : columns)
		{
			list.add(listPropertyMeta.get(pair.right));
		}
		return list;
	}

	public <ID, V> Set<V> loadSetProperty(ID key, GenericDao<ID> dao, SetPropertyMeta<V> setPropertyMeta)
	{

		Composite start = dao.buildCompositeComparatorStart(setPropertyMeta.getPropertyName(), setPropertyMeta.propertyType());
		Composite end = dao.buildCompositeComparatorEnd(setPropertyMeta.getPropertyName(), setPropertyMeta.propertyType());
		List<Pair<Composite, Object>> columns = dao.findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
		Set<V> set = setPropertyMeta.newSetInstance();
		for (Pair<Composite, Object> pair : columns)
		{
			set.add(setPropertyMeta.get(pair.right));
		}
		return set;
	}

	public <ID, K, V> Map<K, V> loadMapProperty(ID key, GenericDao<ID> dao, MapPropertyMeta<K, V> mapPropertyMeta)
	{

		Composite start = dao.buildCompositeComparatorStart(mapPropertyMeta.getPropertyName(), mapPropertyMeta.propertyType());
		Composite end = dao.buildCompositeComparatorEnd(mapPropertyMeta.getPropertyName(), mapPropertyMeta.propertyType());
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

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key, GenericDao<ID> dao, PropertyMeta<V> propertyMeta)
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
				value = this.loadListProperty(key, dao, (ListPropertyMeta<?>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = this.loadSetProperty(key, dao, (SetPropertyMeta<?>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = this.loadMapProperty(key, dao, (MapPropertyMeta<?, ?>) propertyMeta);
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
