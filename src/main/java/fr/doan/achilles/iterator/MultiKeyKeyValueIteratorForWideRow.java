package fr.doan.achilles.iterator;

import java.util.NoSuchElementException;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * CompositeKeyValueIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyKeyValueIteratorForWideRow<K, V> implements KeyValueIterator<K, V>
{
	private ColumnSliceIterator<?, Composite, V> columnSliceIterator;
	private PropertyMeta<K, V> multiKeyWideMapMeta;
	private KeyValueFactory factory = new KeyValueFactory();

	public MultiKeyKeyValueIteratorForWideRow(
			ColumnSliceIterator<?, Composite, V> columnSliceIterator,
			PropertyMeta<K, V> multiKeyWideMapMeta)
	{
		this.columnSliceIterator = columnSliceIterator;
		this.multiKeyWideMapMeta = multiKeyWideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		return this.columnSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.columnSliceIterator.hasNext())
		{
			HColumn<Composite, V> column = this.columnSliceIterator.next();

			keyValue = factory.createForWideRow(multiKeyWideMapMeta, column);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return keyValue;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException(
				"Remove from iterator is not supported. Please use removeValue() or removeValues() instead");
	}

}
