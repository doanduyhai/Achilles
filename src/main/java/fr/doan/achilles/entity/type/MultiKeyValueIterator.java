package fr.doan.achilles.entity.type;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.proxy.EntityWrapperUtil;

/**
 * MultiKeyValueIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyValueIterator<K, V> extends KeyValueIterator<K, V>
{
	private ColumnSliceIterator<?, DynamicComposite, V> columnSliceIterator;
	private List<Method> componentSetters;
	private MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta;

	private EntityWrapperUtil util = new EntityWrapperUtil();

	public MultiKeyValueIterator(ColumnSliceIterator<?, DynamicComposite, V> columnSliceIterator,
			MultiKeyWideMapMeta<K, V> multiKeyWideMapMeta, List<Method> componentSetters)
	{
		super(columnSliceIterator, multiKeyWideMapMeta.getKeySerializer());
		this.columnSliceIterator = columnSliceIterator;
		this.componentSetters = componentSetters;
		this.multiKeyWideMapMeta = multiKeyWideMapMeta;
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<K, V> next()
	{
		KeyValue<K, V> keyValue = null;
		if (this.columnSliceIterator.hasNext())
		{
			HColumn<DynamicComposite, V> column = this.columnSliceIterator.next();

			keyValue = util.buildMultiKey(multiKeyWideMapMeta.getKeyClass(), multiKeyWideMapMeta,
					(HColumn<DynamicComposite, Object>) column, componentSetters);
		}
		else
		{
			throw new NoSuchElementException();
		}
		return keyValue;
	}

}
