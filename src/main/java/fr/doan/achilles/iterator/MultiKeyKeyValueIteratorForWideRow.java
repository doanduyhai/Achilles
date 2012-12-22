package fr.doan.achilles.iterator;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.proxy.EntityWrapperUtil;

/**
 * CompositeKeyValueIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyKeyValueIteratorForWideRow<K, V> implements KeyValueIterator<K, V>
{
	private ColumnSliceIterator<?, Composite, Object> columnSliceIterator;
	private MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta;
	private EntityWrapperUtil util = new EntityWrapperUtil();

	public MultiKeyKeyValueIteratorForWideRow(
			ColumnSliceIterator<?, Composite, Object> columnSliceIterator,
			List<Method> componentSetters, MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta)
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
			HColumn<Composite, Object> column = this.columnSliceIterator.next();

			keyValue = util.buildMultiKeyForComposite(multiKeyWideMapMeta, column);
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
