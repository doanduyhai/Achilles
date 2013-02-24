package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForComposite;
import info.archinnov.achilles.iterator.KeyValueIteratorForDynamicComposite;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForComposite(
			Iterator<HColumn<Composite, V>> columnSliceIterator, PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForComposite<K, V>(columnSliceIterator, propertyMeta);
	}

	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForDynamicComposite(
			Iterator<HColumn<DynamicComposite, String>> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForDynamicComposite<K, V>(columnSliceIterator, propertyMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForDynamicComposite(
			AchillesJoinSliceIterator<ID, DynamicComposite, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForDynamicComposite<K, V>(
				(AchillesJoinSliceIterator) joinColumnSliceIterator, propertyMeta);
	}

	public <ID, JOIN_ID, K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForComposite(
			AchillesJoinSliceIterator<ID, Composite, JOIN_ID, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForComposite<K, V>(joinColumnSliceIterator, propertyMeta);
	}
}
