package fr.doan.achilles.proxy.collection;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;

public class CollectionProxy<E> implements Collection<E>
{
	protected Collection<E> target;
	protected Map<Method, PropertyMeta<?>> dirtyMap;
	protected Method setter;
	protected PropertyMeta<E> propertyMeta;

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
		return this.target.iterator();
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

	public Map<Method, PropertyMeta<?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public void setDirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public void setPropertyMeta(PropertyMeta<E> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
	}

	protected void markDirty()
	{
		if (!dirtyMap.containsKey(this.setter))
		{
			dirtyMap.put(this.setter, this.propertyMeta);
		}
	}
}
