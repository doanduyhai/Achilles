package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AbstractAchillesSliceIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.CounterKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.KeyValueIteratorImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	private static final Logger log = LoggerFactory.getLogger(IteratorFactory.class);

	public <ID, K, V> KeyValueIterator<K, V> createKeyValueIterator(ThriftPersistenceContext<ID> context,
			AbstractAchillesSliceIterator<HColumn<Composite, V>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		log.debug("Create new KeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new KeyValueIteratorImpl<ID, K, V>(context, columnSliceIterator, propertyMeta);
	}

	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createJoinKeyValueIterator(
			ThriftPersistenceContext<ID> context,
			AchillesJoinSliceIterator<ID, ?, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		log.debug("Create new JoinKeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new KeyValueIteratorImpl<ID, K, V>(context, joinColumnSliceIterator, propertyMeta);
	}

	public <ID, K> KeyValueIterator<K, Counter> createCounterKeyValueIterator(
			ThriftPersistenceContext<ID> context,
			AbstractAchillesSliceIterator<HCounterColumn<Composite>> columnSliceIterator,
			PropertyMeta<K, Counter> propertyMeta)
	{
		log.debug("Create new CounterKeyValueIterator for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return new CounterKeyValueIteratorImpl<ID, K>(context, columnSliceIterator, propertyMeta);
	}

}
