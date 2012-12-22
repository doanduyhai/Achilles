package fr.doan.achilles.iterator.factory;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.iterator.DynamicCompositeKeyValueIterator;
import fr.doan.achilles.iterator.DynamicCompositeMultiKeyValueIterator;
import fr.doan.achilles.iterator.KeyValueIterator;
import fr.doan.achilles.iterator.MultiKeyKeyValueIterator;

/**
 * IteratorFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorFactory
{
	public <K, V> KeyValueIterator<K, V> createKeyValueIterator(
			ColumnSliceIterator<?, K, Object> columnSliceIterator, PropertyMeta<K, V> wideMapMeta)
	{
		return new KeyValueIterator<K, V>(columnSliceIterator, wideMapMeta);
	}

	public <K, V> MultiKeyKeyValueIterator<K, V> createMultiKeyKeyValueIterator(
			ColumnSliceIterator<?, Composite, Object> columnSliceIterator,
			List<Method> componentSetters, MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta)
	{
		return new MultiKeyKeyValueIterator<K, V>(columnSliceIterator, componentSetters,
				multiKeyWideMapMeta);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <K, V> DynamicCompositeKeyValueIterator<K, V> createDynamicCompositeKeyValueIterator(
			ColumnSliceIterator<?, DynamicComposite, ?> columnSliceIterator,
			Serializer<?> keySerializer, PropertyMeta<K, V> wideMapMeta)
	{
		return new DynamicCompositeKeyValueIterator<K, V>(
				(ColumnSliceIterator) columnSliceIterator, keySerializer, wideMapMeta);
	}

	public <K, V> DynamicCompositeMultiKeyValueIterator<K, V> createDynamicCompositeMultiKeyKeyValueIterator(
			ColumnSliceIterator<?, DynamicComposite, Object> columnSliceIterator,
			List<Method> componentSetters, MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta)
	{
		return new DynamicCompositeMultiKeyValueIterator<K, V>(columnSliceIterator,
				componentSetters, multiKeyWideMapMeta);
	}

}
