package info.archinnov.achilles.iterator;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * KeyValueIteratorForEntity
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorForDynamicComposite<K, V> implements KeyValueIterator<K, V>
{
	private Iterator<HColumn<DynamicComposite, Object>> achillesSliceIterator;
	private PropertyMeta<K, V> wideMapMeta;
	private KeyValueFactory factory = new KeyValueFactory();

	public KeyValueIteratorForDynamicComposite(
			Iterator<HColumn<DynamicComposite, Object>> columnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.achillesSliceIterator = columnSliceIterator;
		this.wideMapMeta = wideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		return this.achillesSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, Object> column = this.achillesSliceIterator.next();

			keyValue = factory.createKeyValueForDynamicComposite(wideMapMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return keyValue;
	}

	@Override
	public K nextKey()
	{
		K key = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, Object> column = this.achillesSliceIterator.next();
			key = factory.createKeyForDynamicComposite(wideMapMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return key;
	}

	@Override
	public V nextValue()
	{
		V value = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, Object> column = this.achillesSliceIterator.next();
			value = factory.createValueForDynamicComposite(wideMapMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return value;
	}

	@Override
	public Integer nextTtl()
	{
		Integer ttl = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, Object> column = this.achillesSliceIterator.next();
			ttl = factory.createTtlForDynamicComposite(column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return ttl;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException(
				"Remove from iterator is not supported. Please use removeValue() or removeValues() instead");
	}

}
