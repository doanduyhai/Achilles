package fr.doan.achilles.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

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
