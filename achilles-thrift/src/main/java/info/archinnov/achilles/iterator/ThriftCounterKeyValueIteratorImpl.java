package info.archinnov.achilles.iterator;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;

import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCounterKeyValueIteratorImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterKeyValueIteratorImpl<K> implements KeyValueIterator<K, Counter>
{
	private static final Logger log = LoggerFactory
			.getLogger(ThriftCounterKeyValueIteratorImpl.class);

	private ThriftKeyValueFactory factory = new ThriftKeyValueFactory();
	private ThriftPersistenceContext context;

	private ThriftAbstractSliceIterator<HCounterColumn<Composite>> achillesSliceIterator;
	private PropertyMeta<K, Counter> propertyMeta;

	public ThriftCounterKeyValueIteratorImpl(ThriftPersistenceContext context,
			ThriftAbstractSliceIterator<HCounterColumn<Composite>> columnSliceIterator,
			PropertyMeta<K, Counter> propertyMeta)
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
	public KeyValue<K, Counter> next()
	{
		log.trace("Get next key/counter value from the {} ", achillesSliceIterator.type());
		if (this.achillesSliceIterator.hasNext())
		{
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();
			return factory.createCounterKeyValue(context, propertyMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
	}

	@Override
	public K nextKey()
	{
		log.trace("Get next key from the {} ", achillesSliceIterator.type());
		if (this.achillesSliceIterator.hasNext())
		{
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();
			return factory.createCounterKey(propertyMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
	}

	@Override
	public Counter nextValue()
	{
		log.trace("Get next counter value from the {} ", achillesSliceIterator.type());
		if (this.achillesSliceIterator.hasNext())
		{
			HCounterColumn<Composite> column = this.achillesSliceIterator.next();
			return factory.createCounterValue(context, propertyMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
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
