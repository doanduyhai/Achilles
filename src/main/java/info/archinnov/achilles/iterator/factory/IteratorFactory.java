package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.CounterKeyValueIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorImpl;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <ID, K, V> KeyValueIterator<K, V> createKeyValueIterator(PersistenceContext<ID> context,
			Iterator<HColumn<Composite, V>> columnSliceIterator, PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorImpl<ID, K, V>(context, columnSliceIterator, propertyMeta);
	}

	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createKeyValueJoinIterator(
			PersistenceContext<ID> context,
			AchillesJoinSliceIterator<ID, ?, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorImpl<ID, K, V>(context, joinColumnSliceIterator, propertyMeta);
	}

	public <K> KeyValueIterator<K, Long> createCounterKeyValueIterator(
			Iterator<HCounterColumn<Composite>> columnSliceIterator,
			PropertyMeta<K, Long> propertyMeta)
	{
		return new CounterKeyValueIterator<K>(columnSliceIterator, propertyMeta);
	}

}
