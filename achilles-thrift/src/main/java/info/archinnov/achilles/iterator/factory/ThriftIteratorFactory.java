package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftAbstractSliceIterator;
import info.archinnov.achilles.iterator.ThriftCounterKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftKeyValueIteratorImpl;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValueIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftIteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftIteratorFactory
{
	private static final Logger log = LoggerFactory.getLogger(ThriftIteratorFactory.class);

	public <K, V> KeyValueIterator<K, V> createKeyValueIterator(ThriftPersistenceContext context,
			ThriftAbstractSliceIterator<HColumn<Composite, V>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		log.debug("Create new KeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new ThriftKeyValueIteratorImpl<K, V>(context, columnSliceIterator, propertyMeta);
	}

	public <K, V> KeyValueIterator<K, V> createJoinKeyValueIterator(
			ThriftPersistenceContext context,
			ThriftJoinSliceIterator<?, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		log.debug("Create new JoinKeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new ThriftKeyValueIteratorImpl<K, V>(context, joinColumnSliceIterator, propertyMeta);
	}

	public <K> KeyValueIterator<K, Counter> createCounterKeyValueIterator(
			ThriftPersistenceContext context,
			ThriftAbstractSliceIterator<HCounterColumn<Composite>> columnSliceIterator,
			PropertyMeta<K, Counter> propertyMeta)
	{
		log.debug("Create new CounterKeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new ThriftCounterKeyValueIteratorImpl<K>(context, columnSliceIterator, propertyMeta);
	}

}
