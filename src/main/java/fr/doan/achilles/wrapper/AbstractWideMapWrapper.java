package fr.doan.achilles.wrapper;

import java.util.List;

import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.holder.KeyValue;

/**
 * AbstractWideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractWideMapWrapper<K, V> implements WideMap<K, V>
{

	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count)
	{
		return find(start, true, end, true, false, count);
	}

	@Override
	public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count)
	{
		return find(start, false, end, false, false, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverse(K start, K end, int count)
	{
		return find(start, true, end, true, true, count);
	}

	@Override
	public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count)
	{
		return find(start, false, end, false, true, count);
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
	public List<KeyValue<K, V>> findFirst(int count)
	{
		return this.find(null, null, count);
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
	public List<KeyValue<K, V>> findLast(int count)
	{
		return this.findReverse(null, null, count);
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

}
