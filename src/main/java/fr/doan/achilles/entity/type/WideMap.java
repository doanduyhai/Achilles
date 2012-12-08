package fr.doan.achilles.entity.type;

import java.util.List;

/**
 * WideRow
 * 
 * @author DuyHai DOAN
 * 
 */
public interface WideMap<K, V>
{
	public V getValue(K key);

	public void insertValue(K key, V value, int ttl);

	public void insertValue(K key, V value);

	public List<KeyValue<K, V>> findValues(K start, K end, boolean reverse, int count);

	public List<KeyValue<K, V>> findValues(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count);

	public List<KeyValue<K, V>> findValues(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count);

	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

	public void removeValue(K key);

	public void removeValues(K start, K end);

	public void removeValues(K start, K end, boolean inclusiveBounds);

	public void removeValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd);
}
