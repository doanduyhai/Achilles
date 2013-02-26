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
public class EntryIteratorWrapper<K, V> extends AbstractWrapper<K, V> implements
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
		Entry<K, V> entry = this.target.next();
		return builder(entry) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta(propertyMeta) //
				.helper(helper) //
				.build();
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}

}
