package fr.doan.achilles.entity.type;

import java.util.List;

import fr.doan.achilles.holder.KeyValue;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public interface WideRow<K, V>
{
	public V get(K key);

	public void insert(K key, V value, int ttl);

	public void insert(K key, V value);

	public List<KeyValue<K, V>> findRange(K start, K end, boolean reverse, int count);

	public List<KeyValue<K, V>> findRange(K start, K end, boolean inclusiveBounds, boolean reverse,
			int count);

	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

	public void remove(K key);

	public void removeRange(K start, K end);

	public void removeRange(K start, K end, boolean inclusiveBounds);

	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd);
}
