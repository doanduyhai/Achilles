package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.CounterKeyValueIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForComposite;
import info.archinnov.achilles.iterator.KeyValueIteratorForDynamicComposite;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
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
	public <ID, K, V> KeyValueIterator<K, V> createKeyValueIteratorForComposite(
			PersistenceContext<ID> context, Iterator<HColumn<Composite, V>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForComposite<ID, K, V>(context, columnSliceIterator,
				propertyMeta);
	}

	public <ID, K, V> KeyValueIterator<K, V> createKeyValueIteratorForDynamicComposite(
			PersistenceContext<ID> context,
			Iterator<HColumn<DynamicComposite, String>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForDynamicComposite<ID, K, V>(context, columnSliceIterator,
				propertyMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForDynamicComposite(
			PersistenceContext<ID> context,
			AchillesJoinSliceIterator<ID, DynamicComposite, String, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForDynamicComposite<ID, K, V>(context,
				(AchillesJoinSliceIterator) joinColumnSliceIterator, propertyMeta);
	}

	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForComposite(
			PersistenceContext<ID> context,
			AchillesJoinSliceIterator<ID, Composite, ?, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForComposite<ID, K, V>(context, joinColumnSliceIterator,
				propertyMeta);
	}

	public <K> KeyValueIterator<K, Long> createCounterKeyValueIteratorForDynamicComposite(
			Iterator<HCounterColumn<DynamicComposite>> columnSliceIterator,
			PropertyMeta<K, Long> propertyMeta)
	{
		return new CounterKeyValueIterator<K>(columnSliceIterator, propertyMeta);
	}

}
