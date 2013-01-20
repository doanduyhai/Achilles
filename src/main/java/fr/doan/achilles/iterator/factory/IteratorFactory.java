package fr.doan.achilles.iterator.factory;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.iterator.JoinColumnSliceIterator;
import fr.doan.achilles.iterator.KeyValueIteratorForComposite;
import fr.doan.achilles.iterator.KeyValueIteratorForDynamicComposite;
import fr.doan.achilles.iterator.KeyValueJoinIteratorForComposite;
import fr.doan.achilles.iterator.KeyValueJoinIteratorForDynamicComposite;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForComposite(
			ColumnSliceIterator<?, Composite, ?> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForComposite<K, V>(columnSliceIterator, propertyMeta);
	}

	public <K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForComposite(
			JoinColumnSliceIterator<?, Composite, ?, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueJoinIteratorForComposite<K, V>(joinColumnSliceIterator, propertyMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForDynamicComposite(
			ColumnSliceIterator<?, DynamicComposite, ?> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForDynamicComposite<K, V>(
				(ColumnSliceIterator) columnSliceIterator, propertyMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <K, V> KeyValueIterator<K, V> createKeyValueJoinIteratorForDynamicComposite(
			JoinColumnSliceIterator<?, DynamicComposite, ?, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueJoinIteratorForDynamicComposite<K, V>(
				(JoinColumnSliceIterator) joinColumnSliceIterator, propertyMeta);
	}
}
