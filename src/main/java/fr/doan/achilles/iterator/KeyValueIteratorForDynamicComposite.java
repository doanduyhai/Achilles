package fr.doan.achilles.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.DynamicCompositeTransformer;

/**
 * KeyValueIteratorForEntity
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorForDynamicComposite<K, V> implements KeyValueIterator<K, V>
{
	private Iterator<HColumn<DynamicComposite, Object>> achillesSliceIterator;
	private PropertyMeta<K, V> wideMapMeta;
	private DynamicCompositeTransformer transformer = new DynamicCompositeTransformer();

	public KeyValueIteratorForDynamicComposite(
			Iterator<HColumn<DynamicComposite, Object>> columnSliceIterator,
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
			HColumn<DynamicComposite, Object> column = this.achillesSliceIterator.next();

			keyValue = transformer.buildKeyValueFromDynamicComposite(wideMapMeta, column);
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
