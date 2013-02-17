package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;

import java.util.List;

/**
 * AbstractWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractWideMapWrapper<K, V> implements WideMap<K, V>
{

	protected AchillesInterceptor interceptor;

	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count)
	{
		return find(start, true, end, true, false, count);
	}

	@Override
	public List<V> findValues(K start, K end, int count)
	{
		return findValues(start, true, end, true, false, count);
	}

	@Override
	public List<K> findKeys(K start, K end, int count)
	{
		return findKeys(start, true, end, true, false, count);
	}

	@Override
	public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count)
	{
		return find(start, false, end, false, false, count);
	}

	@Override
	public List<V> findValuesBoundsExclusive(K start, K end, int count)
	{
		return findValues(start, false, end, false, false, count);
	}

	@Override
	public List<K> findKeysBoundsExclusive(K start, K end, int count)
	{
		return findKeys(start, false, end, false, false, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverse(K start, K end, int count)
	{
		return find(start, true, end, true, true, count);
	}

	@Override
	public List<V> findValuesReverse(K start, K end, int count)
	{
		return findValues(start, true, end, true, true, count);
	}

	@Override
	public List<K> findKeysReverse(K start, K end, int count)
	{
		return findKeys(start, true, end, true, true, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count)
	{
		return find(start, false, end, false, true, count);
	}

	@Override
	public List<V> findValuesReverseBoundsExclusive(K start, K end, int count)
	{
		return findValues(start, false, end, false, true, count);
	}

	@Override
	public List<K> findKeysReverseBoundsExclusive(K start, K end, int count)
	{
		return findKeys(start, false, end, false, true, count);
	}

	@Override
	public KeyValue<K, V> findFirst()
	{
		List<KeyValue<K, V>> result = this.find(null, null, 1);

		KeyValue<K, V> keyValue = null;
		if (result.size() > 0)
		{
			keyValue = result.get(0);
		}

		return keyValue;
	}

	@Override
	public V findValuesFirst()
	{
		List<V> result = this.findValues(null, null, 1);

		V value = null;
		if (result.size() > 0)
		{
			value = result.get(0);
		}

		return value;
	}

	@Override
	public K findKeysFirst()
	{
		List<K> result = this.findKeys(null, null, 1);

		K key = null;
		if (result.size() > 0)
		{
			key = result.get(0);
		}

		return key;
	}

	@Override
	public List<KeyValue<K, V>> findFirst(int count)
	{
		return this.find(null, null, count);
	}

	@Override
	public List<V> findValuesFirst(int count)
	{
		return this.findValues(null, null, count);
	}

	@Override
	public List<K> findKeysFirst(int count)
	{
		return this.findKeys(null, null, count);
	}

	@Override
	public KeyValue<K, V> findLast()
	{
		List<KeyValue<K, V>> result = this.findReverse(null, null, 1);

		KeyValue<K, V> keyValue = null;
		if (result.size() > 0)
		{
			keyValue = result.get(0);
		}

		return keyValue;
	}

	@Override
	public V findValuesLast()
	{
		List<V> result = this.findValuesReverse(null, null, 1);

		V value = null;
		if (result.size() > 0)
		{
			value = result.get(0);
		}

		return value;
	}

	@Override
	public K findKeysLast()
	{
		List<K> result = this.findKeysReverse(null, null, 1);

		K key = null;
		if (result.size() > 0)
		{
			key = result.get(0);
		}

		return key;
	}

	@Override
	public List<KeyValue<K, V>> findLast(int count)
	{
		return this.findReverse(null, null, count);
	}

	@Override
	public List<V> findValuesLast(int count)
	{
		return this.findValuesReverse(null, null, count);
	}

	@Override
	public List<K> findKeysLast(int count)
	{
		return this.findKeysReverse(null, null, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count)
	{
		return iterator(start, true, end, true, false, count);
	}

	@Override
	public KeyValueIterator<K, V> iteratorBoundsExclusive(K start, K end, int count)
	{
		return iterator(start, false, end, false, false, count);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(K start, K end, int count)
	{
		return iterator(start, true, end, true, true, count);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(K start, K end, int count)
	{
		return iterator(start, false, end, false, true, count);
	}

	@Override
	public void remove(K start, K end)
	{
		remove(start, true, end, true);
	}

	@Override
	public void removeBoundsExclusive(K start, K end)
	{
		remove(start, false, end, false);
	}

	@Override
	public void removeFirst()
	{
		removeFirst(1);
	}

	@Override
	public void removeLast()
	{
		removeLast(1);
	}

	public AchillesInterceptor getInterceptor()
	{
		return interceptor;
	}

	public void setInterceptor(AchillesInterceptor interceptor)
	{
		this.interceptor = interceptor;
	}
}
