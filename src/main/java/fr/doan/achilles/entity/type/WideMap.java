package fr.doan.achilles.entity.type;

import java.util.List;

import fr.doan.achilles.holder.KeyValue;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public interface WideMap<K, V>
{
	public V get(K key);

	public void insert(K key, V value, int ttl);

	public void insert(K key, V value);

	public List<KeyValue<K, V>> find(K start, K end, int count);

	public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count);

	public List<KeyValue<K, V>> findReverse(K start, K end, int count);

	public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count);

	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count);

	public KeyValue<K, V> findFirst();

	public KeyValue<K, V> findLast();

	public KeyValueIterator<K, V> iterator(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorBoundsExclusive(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorReverse(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(K start, K end, int count);

	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

	public void remove(K key);

	public void remove(K start, K end);

	public void removeBoundsExclusive(K start, K end);

	public void remove(K start, boolean inclusiveStart, K end, boolean inclusiveEnd);

	public void removeFirst();

	public void removeLast();
}
