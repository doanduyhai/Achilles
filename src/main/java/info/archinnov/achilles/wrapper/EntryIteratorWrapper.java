package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.wrapper.builder.MapEntryWrapperBuilder.builder;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * EntryIteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntryIteratorWrapper<ID, K, V> extends AbstractWrapper<ID, K, V> implements
		Iterator<Entry<K, V>>
{

	private Iterator<Entry<K, V>> target;

	public EntryIteratorWrapper(Iterator<Entry<K, V>> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext()
	{
		return this.target.hasNext();
	}

	@Override
	public Entry<K, V> next()
	{
		Entry<K, V> result = null;
		Entry<K, V> entry = this.target.next();
		if (entry != null)
		{
			result = builder(context, entry) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta(propertyMeta) //
					.proxifier(proxifier) //
					.build();
		}
		return result;
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}

}
