package fr.doan.achilles.iterator;

import java.util.NoSuchElementException;

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
public class KeyValueJoinIteratorForDynamicComposite<K, V> implements KeyValueIterator<K, V>
{
	private JoinColumnSliceIterator<?, DynamicComposite, Object, K, V> joinColumnSliceIterator;
	private PropertyMeta<K, V> wideMapMeta;
	private KeyValueFactory factory = new KeyValueFactory();

	public KeyValueJoinIteratorForDynamicComposite(
			JoinColumnSliceIterator<?, DynamicComposite, Object, K, V> columnSliceIterator,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.joinColumnSliceIterator = columnSliceIterator;
		this.wideMapMeta = wideMapMeta;
	}

	@Override
	public boolean hasNext()
	{
		return this.joinColumnSliceIterator.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.joinColumnSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, Object> column = (HColumn<DynamicComposite, Object>) this.joinColumnSliceIterator
					.next();

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
