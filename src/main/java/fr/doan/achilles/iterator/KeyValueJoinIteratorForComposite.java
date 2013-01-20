package fr.doan.achilles.iterator;

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
public class KeyValueJoinIteratorForComposite<K, V> implements KeyValueIterator<K, V>
{
	protected JoinColumnSliceIterator<?, Composite, ?, K, V> joinColumnSliceIterator;
	private KeyValueFactory factory = new KeyValueFactory();
	private PropertyMeta<K, V> wideMapMeta;

	protected KeyValueJoinIteratorForComposite() {}

	public KeyValueJoinIteratorForComposite(
			JoinColumnSliceIterator<?, Composite, ?, K, V> joinColumnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.joinColumnSliceIterator = joinColumnSliceIterator;
		this.wideMapMeta = wideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		return this.joinColumnSliceIterator.hasNext();
	}

	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.joinColumnSliceIterator.hasNext())
		{
			HColumn<Composite, ?> column = this.joinColumnSliceIterator.next();
			keyValue = factory.createForComposite(wideMapMeta, column);
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
