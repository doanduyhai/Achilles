package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap;
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

	private static final int DEFAULT_COUNT = 100;

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
	public List<V> findBoundsExclusiveValues(K start, K end, int count)
	{
		return findValues(start, false, end, false, false, count);
	}

	@Override
	public List<K> findBoundsExclusiveKeys(K start, K end, int count)
	{
		return findKeys(start, false, end, false, false, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverse(K start, K end, int count)
	{
		return find(start, true, end, true, true, count);
	}

	@Override
	public List<V> findReverseValues(K start, K end, int count)
	{
		return findValues(start, true, end, true, true, count);
	}

	@Override
	public List<K> findReverseKeys(K start, K end, int count)
	{
		return findKeys(start, true, end, true, true, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count)
	{
		return find(start, false, end, false, true, count);
	}

	@Override
	public List<V> findReverseBoundsExclusiveValues(K start, K end, int count)
	{
		return findValues(start, false, end, false, true, count);
	}

	@Override
	public List<K> findReverseBoundsExclusiveKeys(K start, K end, int count)
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
	public V findFirstValue()
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
	public K findFirstKey()
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
	public List<V> findFirstValues(int count)
	{
		return this.findValues(null, null, count);
	}

	@Override
	public List<K> findFirstKeys(int count)
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
	public V findLastValue()
	{
		List<V> result = this.findReverseValues(null, null, 1);

		V value = null;
		if (result.size() > 0)
		{
			value = result.get(0);
		}

		return value;
	}

	@Override
	public K findLastKey()
	{
		List<K> result = this.findReverseKeys(null, null, 1);

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
	public List<V> findLastValues(int count)
	{
		return this.findReverseValues(null, null, count);
	}

	@Override
	public List<K> findLastKeys(int count)
	{
		return this.findReverseKeys(null, null, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator()
	{
		return iterator(null, true, null, true, false, DEFAULT_COUNT);
	}

	@Override
	public KeyValueIterator<K, V> iterator(int count)
	{
		return iterator(null, true, null, true, false, count);
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
	public KeyValueIterator<K, V> iteratorReverse()
	{
		return iterator(null, true, null, true, true, DEFAULT_COUNT);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(int count)
	{
		return iterator(null, true, null, true, true, count);
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
