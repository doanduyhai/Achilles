package fr.doan.achilles.entity.type;

import java.util.Iterator;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * KeyValueIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIterator<K, V> implements Iterator<KeyValue<K, V>>
{
	private final ColumnSliceIterator<?, DynamicComposite, V> columnSliceIterator;
	private final Serializer<?> keySerializer;

	public KeyValueIterator(ColumnSliceIterator<?, DynamicComposite, V> columnSliceIterator, Serializer<?> keySerializer) {
		this.columnSliceIterator = columnSliceIterator;
		this.keySerializer = keySerializer;
	}

	@Override
	public boolean hasNext()
	{
		return this.columnSliceIterator.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.columnSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, V> column = this.columnSliceIterator.next();

			DynamicComposite composite = column.getName();
			K key = (K) composite.get(2, this.keySerializer);

			keyValue = new KeyValue<K, V>(key, column.getValue(), column.getTtl());
		}
		return keyValue;
	}

	@Override
	public void remove()
	{
		this.columnSliceIterator.remove();
	}

}
