package fr.doan.achilles.entity.type;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;

/**
 * KeyValue
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValue<K, V>
{
	private final K key;
	private final V value;
	private final int ttl;

	public KeyValue(K key, V value, int ttl) {
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	public KeyValue(K key, V value) {
		this.key = key;
		this.value = value;
		this.ttl = 0;
	}

	@SuppressWarnings("unchecked")
	public KeyValue(HColumn<DynamicComposite, Object> hColumn, Serializer<?> keySerializer,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.key = (K) hColumn.getName().get(2, keySerializer);
		this.value = wideMapMeta.getValue(hColumn.getValue());
		this.ttl = hColumn.getTtl();
	}

	public static <K, V> List<KeyValue<K, V>> fromListOfDynamicCompositeHColums(
			List<HColumn<DynamicComposite, Object>> hColumns, Serializer<?> keySerializer,
			PropertyMeta<K, V> wideMapMeta)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<DynamicComposite, Object> hColumn : hColumns)
			{
				result.add(new KeyValue<K, V>(hColumn, keySerializer, wideMapMeta));
			}
		}
		return result;
	}

	public static <K, V> List<KeyValue<K, V>> fromListOfSimpleHColums(
			List<HColumn<K, Object>> hColumns, Serializer<?> keySerializer,
			PropertyMeta<K, V> wideMapMeta)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<K, Object> hColumn : hColumns)
			{
				V value = wideMapMeta.getValue(hColumn.getValue());
				result.add(new KeyValue<K, V>(hColumn.getName(), value, hColumn.getTtl()));
			}
		}
		return result;
	}

	public K getKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}

	public int getTtl()
	{
		return ttl;
	}
}
