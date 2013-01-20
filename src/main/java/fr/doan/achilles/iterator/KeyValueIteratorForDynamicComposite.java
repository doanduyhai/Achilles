package fr.doan.achilles.iterator;

import java.util.NoSuchElementException;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * KeyValueIteratorForEntity
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorForDynamicComposite<K, V> implements KeyValueIterator<K, V>
{
	private ColumnSliceIterator<?, DynamicComposite, Object> columnSliceIterator;
	private PropertyMeta<K, V> wideMapMeta;
	private KeyValueFactory factory = new KeyValueFactory();

	public KeyValueIteratorForDynamicComposite(
			ColumnSliceIterator<?, DynamicComposite, Object> columnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.columnSliceIterator = columnSliceIterator;
		this.wideMapMeta = wideMapMeta;
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
			HColumn<DynamicComposite, Object> column = this.columnSliceIterator.next();

			keyValue = factory.createForDynamicComposite(wideMapMeta, column);
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
