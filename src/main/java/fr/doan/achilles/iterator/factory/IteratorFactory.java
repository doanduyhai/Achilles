package fr.doan.achilles.iterator.factory;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.iterator.KeyValueIteratorForEntity;
import fr.doan.achilles.iterator.KeyValueIteratorForWideRow;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForWideRow(
			ColumnSliceIterator<?, Composite, V> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForWideRow<K, V>(columnSliceIterator, propertyMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <K, V> KeyValueIterator<K, V> createKeyValueIteratorForEntity(
			ColumnSliceIterator<?, DynamicComposite, ?> columnSliceIterator,
			PropertyMeta<K, V> propertyMeta)
	{
		return new KeyValueIteratorForEntity<K, V>((ColumnSliceIterator) columnSliceIterator,
				propertyMeta);
	}
}
