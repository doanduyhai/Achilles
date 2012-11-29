package fr.doan.achilles.wrapper;

import java.util.Iterator;
import java.util.Map.Entry;

import fr.doan.achilles.wrapper.builder.MapEntryProxyBuilder;

public class EntryIteratorProxy<K, V> extends AbstractProxy<V> implements Iterator<Entry<K, V>>
{

	private Iterator<Entry<K, V>> target;

	public EntryIteratorProxy(Iterator<Entry<K, V>> target) {
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
		return MapEntryProxyBuilder.builder(entry).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}

}
