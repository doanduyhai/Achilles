package info.archinnov.achilles.iterator;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * KeyValueIteratorForWideRow
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorForComposite<K, V> implements KeyValueIterator<K, V>
{
	// protected AchillesSliceIterator<?, Composite, ?> achillesSliceIterator;
	protected Iterator<HColumn<Composite, V>> achillesSliceIterator;
	private KeyValueFactory factory = new KeyValueFactory();
	private PropertyMeta<K, V> wideMapMeta;

	protected KeyValueIteratorForComposite() {}

	public KeyValueIteratorForComposite(Iterator<HColumn<Composite, V>> columnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.achillesSliceIterator = columnSliceIterator;
		this.wideMapMeta = wideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		return this.achillesSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.achillesSliceIterator.hasNext())
		{
			HColumn<Composite, ?> column = this.achillesSliceIterator.next();
			keyValue = factory.createKeyValueForComposite(wideMapMeta, column);
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
