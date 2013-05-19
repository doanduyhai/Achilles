package info.archinnov.achilles.iterator;

import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeyValueIteratorForComposite
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorImpl<K, V> implements KeyValueIterator<K, V>
{
	private static final Logger log = LoggerFactory.getLogger(KeyValueIteratorImpl.class);

	private KeyValueFactory factory = new KeyValueFactory();
	protected AbstractAchillesSliceIterator<HColumn<Composite, V>> achillesSliceIterator;
	private PropertyMeta<K, V> propertyMeta;
	private ThriftPersistenceContext context;

	protected KeyValueIteratorImpl() {}

	public KeyValueIteratorImpl(ThriftPersistenceContext context,
			AbstractAchillesSliceIterator<HColumn<Composite, V>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		this.context = context;
		this.achillesSliceIterator = columnSliceIterator;
		this.propertyMeta = propertyMeta;
	}

	@Override
	public boolean hasNext()
	{
		log.trace("Does the {} has next value ? ", achillesSliceIterator.type());
		return this.achillesSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, V> next()
	{
		log.trace("Get next key/value from the {} ", achillesSliceIterator.type());
		KeyValue<K, V> keyValue = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<Composite, V> column = this.achillesSliceIterator.next();
			keyValue = factory.createKeyValue(context, propertyMeta, column);
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
		log.trace("Get next key from the {} ", achillesSliceIterator.type());
		K key = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<Composite, ?> column = this.achillesSliceIterator.next();
			key = factory.createKey(propertyMeta, column);
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
		log.trace("Get next value from the {} ", achillesSliceIterator.type());
		V value = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<Composite, V> column = this.achillesSliceIterator.next();
			value = factory.createValue(context, propertyMeta, column);
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
		log.trace("Get next ttl from the {} ", achillesSliceIterator.type());
		Integer ttl = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<Composite, ?> column = this.achillesSliceIterator.next();
			ttl = factory.createTtl(column);
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
