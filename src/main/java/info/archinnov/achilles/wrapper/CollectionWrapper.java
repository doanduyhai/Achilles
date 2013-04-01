package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.wrapper.builder.IteratorWrapperBuilder.builder;

import java.util.Collection;
import java.util.Iterator;

/**
 * CollectionWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CollectionWrapper<ID, V> extends AbstractWrapper<ID, Void, V> implements Collection<V>
{
	protected Collection<V> target;

	public CollectionWrapper(Collection<V> target) {
		this.target = target;
	}

	@Override
	public boolean add(V arg0)
	{
		this.markDirty();
		return target.add(proxifier.unproxy(arg0));
	}

	@Override
	public boolean addAll(Collection<? extends V> arg0)
	{
		boolean result = false;
		result = target.addAll(proxifier.unproxy(arg0));
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
		return this.target.contains(proxifier.unproxy(arg0));
	}

	@Override
	public boolean containsAll(Collection<?> arg0)
	{
		return this.target.containsAll(proxifier.unproxy(arg0));
	}

	@Override
	public boolean isEmpty()
	{
		return this.target.isEmpty();
	}

	@Override
	public Iterator<V> iterator()
	{
		return builder(context, this.target.iterator()) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta(propertyMeta) //
				.proxifier(proxifier) //
				.build();
	}

	@Override
	public boolean remove(Object arg0)
	{
		boolean result = false;
		result = this.target.remove(proxifier.unproxy(arg0));
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> arg0)
	{
		boolean result = false;
		result = this.target.removeAll(proxifier.unproxy(arg0));
		if (result)
		{
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> arg0)
	{
		boolean result = false;
		result = this.target.retainAll(proxifier.unproxy(arg0));
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
		Object[] result = null;
		if (isJoin())
		{
			Object[] array = new Object[this.target.size()];
			int i = 0;
			for (V joinEntity : this.target)
			{
				array[i] = proxifier.buildProxy(joinEntity, joinContext(joinEntity));
				i++;
			}
			result = array;
		}
		else
		{
			result = this.target.toArray();
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] arg0)
	{
		T[] result = null;
		if (isJoin())
		{
			T[] array = this.target.toArray(arg0);

			for (int i = 0; i < array.length; i++)
			{
				array[i] = proxifier.buildProxy(array[i], joinContext((V) array[i]));
			}
			result = array;
		}
		else
		{
			result = this.target.toArray(arg0);
		}
		return result;
	}

	public Collection<V> getTarget()
	{
		return this.target;
	}
}
