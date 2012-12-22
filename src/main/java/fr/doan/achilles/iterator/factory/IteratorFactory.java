package fr.doan.achilles.iterator.factory;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.iterator.KeyValueIteratorForEntity;
import fr.doan.achilles.iterator.KeyValueIteratorForWideRow;
import fr.doan.achilles.iterator.MultiKeyKeyValueIteratorForEntity;
import fr.doan.achilles.iterator.MultiKeyKeyValueIteratorForWideRow;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <K, V> KeyValueIteratorForWideRow<K, V> createKeyValueIteratorForWideRow(
			ColumnSliceIterator<?, Composite, Object> columnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		return new KeyValueIteratorForWideRow<K, V>(columnSliceIterator, wideMapMeta);
	}

	public <K, V> MultiKeyKeyValueIteratorForWideRow<K, V> createMultiKeyKeyValueIteratorForWideRow(
			ColumnSliceIterator<?, Composite, Object> columnSliceIterator,
			List<Method> componentSetters, MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta)
	{
		return new MultiKeyKeyValueIteratorForWideRow<K, V>(columnSliceIterator, componentSetters,
				multiKeyWideMapMeta);
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
		if (propertyMeta.isSingleKey())
		{
			return new KeyValueIteratorForEntity<K, V>((ColumnSliceIterator) columnSliceIterator,
					propertyMeta);
		}
		else
		{
			return new MultiKeyKeyValueIteratorForEntity<K, V>(
					(ColumnSliceIterator) columnSliceIterator, propertyMeta);
		}
	}
}
