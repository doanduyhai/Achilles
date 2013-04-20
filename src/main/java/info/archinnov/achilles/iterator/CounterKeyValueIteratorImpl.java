package info.archinnov.achilles.iterator;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CounterKeyValueIteratorImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterKeyValueIteratorImpl<K> implements KeyValueIterator<K, Long>
{
	private static final Logger log = LoggerFactory.getLogger(CounterKeyValueIteratorImpl.class);

	private KeyValueFactory factory = new KeyValueFactory();

	private AbstractAchillesSliceIterator<HCounterColumn<Composite>> achillesSliceIterator;
	private PropertyMeta<K, Long> propertyMeta;

	public CounterKeyValueIteratorImpl(
			AbstractAchillesSliceIterator<HCounterColumn<Composite>> columnSliceIterator,
			PropertyMeta<K, Long> wideMapMeta)
	{
		this.achillesSliceIterator = columnSliceIterator;
		this.propertyMeta = wideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		log.trace("Does the {} has next value ? ", achillesSliceIterator.type());
		return this.achillesSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, Long> next()
	{
		log.trace("Get next key/counter value from the {} ", achillesSliceIterator.type());
		KeyValue<K, Long> keyValue = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();

			keyValue = factory.createCounterKeyValue(propertyMeta, column);
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
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();
			key = factory.createCounterKey(propertyMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return key;
	}

	@Override
	public Long nextValue()
	{
		log.trace("Get next counter value from the {} ", achillesSliceIterator.type());
		Long value = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();
			value = factory.createCounterValue(propertyMeta, column);
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
		throw new UnsupportedOperationException("Ttl does not exist for counter type");
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

}
