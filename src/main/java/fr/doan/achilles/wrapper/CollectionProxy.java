package fr.doan.achilles.wrapper;

import java.util.Collection;
import java.util.Iterator;

import fr.doan.achilles.wrapper.builder.IteratorProxyBuilder;

public class CollectionProxy<E> extends AbstractProxy<E> implements Collection<E>
{
	protected Collection<E> target;

	public CollectionProxy(Collection<E> target) {
		this.target = target;
	}

	@Override
	public boolean add(E arg0)
	{
		boolean result = target.add(arg0);
		this.markDirty();
		return result;
	}

	@Override
	public boolean addAll(Collection<? extends E> arg0)
	{
		boolean result = target.addAll(arg0);
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public void clear()
	{

		if (this.target.size() > 0)
		{
			this.markDirty();
		}
		this.target.clear();
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
	public Iterator<E> iterator()
	{
		return IteratorProxyBuilder.builder(this.target.iterator()).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta)
				.build();
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

	public Collection<E> getTarget()
	{
		return this.target;
	}
}
