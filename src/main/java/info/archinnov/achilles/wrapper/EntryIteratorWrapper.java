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
		boolean result = false;
		if (target != null)
		{
			result = this.target.hasNext();
		}
		return result;
	}

	@Override
	public Entry<K, V> next()
	{
		Entry<K, V> result = null;
		if (target != null)
		{
			Entry<K, V> entry = this.target.next();
			result = builder(context, entry) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta(propertyMeta) //
					.helper(helper) //
					.build();

		}
		return result;
	}

	@Override
	public void remove()
	{
		if (target != null)
		{
			this.target.remove();
			this.markDirty();
		}
	}

}
