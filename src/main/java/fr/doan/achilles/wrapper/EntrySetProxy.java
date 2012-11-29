package fr.doan.achilles.wrapper;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import fr.doan.achilles.wrapper.builder.EntryIteratorProxyBuilder;

public class EntrySetProxy<K, V> extends AbstractProxy<V> implements Set<Entry<K, V>>
{

	private Set<Entry<K, V>> target;

	public EntrySetProxy(Set<Entry<K, V>> target) {
		this.target = target;
	}

	@Override
	public boolean add(Entry<K, V> arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for an Entry set");
	}

	@Override
	public boolean addAll(Collection<? extends Entry<K, V>> arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for an Entry set");
	}

	@Override
	public void clear()
	{
		this.target.clear();
		this.markDirty();
	}

	@Override
	public boolean contains(Object arg0)
	{
		return this.target.contains(arg0);
	}

	@Override
	public boolean containsAll(Collection<?> arg0)
	{
		return this.target.containsAll(arg0);
	}

	@Override
	public boolean isEmpty()
	{
		return this.target.isEmpty();
	}

	@Override
	public Iterator<Entry<K, V>> iterator()
	{
		return EntryIteratorProxyBuilder.builder(this.target.iterator()).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();
	}

	@Override
	public boolean remove(Object arg0)
	{
		boolean result = this.target.remove(arg0);
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> arg0)
	{
		boolean result = this.target.removeAll(arg0);
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> arg0)
	{
		boolean result = this.target.retainAll(arg0);
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public int size()
	{
		return this.target.size();
	}

	@Override
	public Object[] toArray()
	{
		return this.target.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0)
	{
		return this.target.toArray(arg0);
	}

}
