package fr.doan.achilles.holder.factory;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	public <K, V> KeyValue<K, V> create(K key, V value, int ttl)
	{
		return new KeyValue<K, V>(key, value, ttl);
	}

	public <K, V> KeyValue<K, V> create(K key, V value)
	{
		return new KeyValue<K, V>(key, value);
	}

	@SuppressWarnings("unchecked")
	public <K, V> KeyValue<K, V> createFromDynamicCompositeColumn(
			HColumn<DynamicComposite, Object> hColumn, Serializer<?> keySerializer,
			PropertyMeta<K, V> wideMapMeta)
	{
		K key = (K) hColumn.getName().get(2, keySerializer);
		V value = wideMapMeta.getValue(hColumn.getValue());
		int ttl = hColumn.getTtl();

		return create(key, value, ttl);
	}

	public <K, V> List<KeyValue<K, V>> createFromDynamicCompositeColumnList(
			List<HColumn<DynamicComposite, Object>> hColumns, Serializer<?> keySerializer,
			PropertyMeta<K, V> wideMapMeta)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<DynamicComposite, Object> hColumn : hColumns)
			{
				result.add(createFromDynamicCompositeColumn(hColumn, keySerializer, wideMapMeta));
			}
		}
		return result;
	}

	public <K, V> List<KeyValue<K, V>> createFromColumnList(List<HColumn<K, Object>> hColumns,
			Serializer<?> keySerializer, PropertyMeta<K, V> wideMapMeta)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<K, Object> hColumn : hColumns)
			{
				V value = wideMapMeta.getValue(hColumn.getValue());
				result.add(create(hColumn.getName(), value, hColumn.getTtl()));
			}
		}
		return result;
	}
}
