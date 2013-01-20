package fr.doan.achilles.holder.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.holder.KeyValue;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private EntityLoader loader = new EntityLoader();
	private PropertyHelper helper = new PropertyHelper();

	public <K, V> KeyValue<K, V> create(K key, V value, int ttl)
	{
		return new KeyValue<K, V>(key, value, ttl);
	}

	public <K, V> KeyValue<K, V> create(K key, V value)
	{
		return new KeyValue<K, V>(key, value);
	}

	// Dynamic Composite

	public <K, V> KeyValue<K, V> createForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key = buildKeyFromDynamicComposite(propertyMeta, hColumn);
		V value = propertyMeta.getValue(hColumn.getValue());
		int ttl = hColumn.getTtl();

		return create(key, value, ttl);
	}

	public <K, V> List<KeyValue<K, V>> createListForDynamicComposite(
			PropertyMeta<K, V> propertyMeta, List<HColumn<DynamicComposite, Object>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		if (hColumns != null && hColumns.size() > 0)
		{
			if (propertyMeta.isJoinColumn())
			{
				loadJoinEntitiesFromDynamicComposite(propertyMeta, hColumns, result);
			}
			else
			{

				for (HColumn<DynamicComposite, Object> hColumn : hColumns)
				{
					result.add(createForDynamicComposite(propertyMeta, hColumn));
				}
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private <V, K> void loadJoinEntitiesFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, Object>> hColumns, List<KeyValue<K, V>> result)
	{
		Map<Object, Pair<K, Integer>> joinIdMap = new HashMap<Object, Pair<K, Integer>>();
		List<Object> joinIds = new ArrayList<Object>();
		for (HColumn<DynamicComposite, Object> hColumn : hColumns)
		{
			K key = buildKeyFromDynamicComposite(propertyMeta, hColumn);
			joinIdMap.put(hColumn.getValue(), new Pair<K, Integer>(key, hColumn.getTtl()));
			joinIds.add(hColumn.getValue());
		}

		EntityMeta<Object> joinEntityMeta = propertyMeta.getJoinProperties().getEntityMeta();

		Map<Object, V> loadedEntities = loader.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, joinEntityMeta);

		for (Object joinId : joinIds)
		{
			Pair<K, Integer> pair = joinIdMap.get(joinId);
			K key = pair.left;
			Integer ttl = pair.right;
			result.add(new KeyValue<K, V>(key, loadedEntities.get(joinId), ttl));

		}
	}

	private <K, V> K buildKeyFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(2, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper.buildMultiKeyForDynamicComposite(propertyMeta, hColumn.getName()
					.getComponents());
		}
		return key;
	}

	// Composite

	public <K, V> KeyValue<K, V> createForComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		K key = buildKeyFromComposite(propertyMeta, hColumn);
		V value = propertyMeta.getValue(hColumn.getValue());
		int ttl = hColumn.getTtl();
		return create(key, value, ttl);
	}

	public <K, V> List<KeyValue<K, V>> createListForComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		if (hColumns != null && hColumns.size() > 0)
		{
			if (propertyMeta.isJoinColumn())
			{
				loadJoinEntitiesFromComposite(propertyMeta, hColumns, result);
			}
			else
			{

				for (HColumn<Composite, ?> hColumn : hColumns)
				{
					result.add(createForComposite(propertyMeta, hColumn));
				}
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private <V, K> void loadJoinEntitiesFromComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns, List<KeyValue<K, V>> result)
	{
		Map<Object, Pair<K, Integer>> joinIdMap = new HashMap<Object, Pair<K, Integer>>();
		List<Object> joinIds = new ArrayList<Object>();
		for (HColumn<Composite, ?> hColumn : hColumns)
		{
			joinIdMap.put(
					hColumn.getValue(),
					new Pair<K, Integer>(buildKeyFromComposite(propertyMeta, hColumn), hColumn
							.getTtl()));
			joinIds.add(hColumn.getValue());
		}

		EntityMeta<Object> joinEntityMeta = propertyMeta.getJoinProperties().getEntityMeta();

		Map<Object, V> loadedEntities = loader.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, joinEntityMeta);

		for (Object joinId : joinIds)
		{
			Pair<K, Integer> pair = joinIdMap.get(joinId);
			K key = pair.left;
			Integer ttl = pair.right;
			result.add(new KeyValue<K, V>(key, loadedEntities.get(joinId), ttl));
		}
	}

	private <K, V> K buildKeyFromComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(0, propertyMeta.getKeySerializer());

		}
		else
		{
			key = helper.buildMultiKeyForComposite(propertyMeta, hColumn.getName().getComponents());
		}
		return key;
	}
}
