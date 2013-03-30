package info.archinnov.achilles.iterator;

import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

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
public class KeyValueIteratorForDynamicComposite<ID, K, V> implements KeyValueIterator<K, V>
{
	private KeyValueFactory factory = new KeyValueFactory();

	private Iterator<HColumn<DynamicComposite, String>> achillesSliceIterator;
	private PropertyMeta<K, V> propertyMeta;
	private PersistenceContext<ID> context;

	public KeyValueIteratorForDynamicComposite(PersistenceContext<ID> context,
			Iterator<HColumn<DynamicComposite, String>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		this.context = context;
		this.achillesSliceIterator = columnSliceIterator;
		this.propertyMeta = propertyMeta;
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
			HColumn<DynamicComposite, String> column = this.achillesSliceIterator.next();

			keyValue = factory.createKeyValueForDynamicComposite(context, propertyMeta, column);
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
			HColumn<DynamicComposite, String> column = this.achillesSliceIterator.next();
			key = factory.createKeyForDynamicComposite(propertyMeta, column);
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
			HColumn<DynamicComposite, String> column = this.achillesSliceIterator.next();
			value = factory.createValueForDynamicComposite(context, propertyMeta, column);
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
			HColumn<DynamicComposite, String> column = this.achillesSliceIterator.next();
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
