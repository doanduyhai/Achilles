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
public class CollectionWrapper<V> extends AbstractWrapper<Void, V> implements Collection<V>
{
	protected Collection<V> target;

	public CollectionWrapper(Collection<V> target) {
		this.target = target;
	}

	@Override
	public boolean add(V arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = target.add(helper.unproxy(arg0));
			this.markDirty();

		}
		return result;
	}

	@Override
	public boolean addAll(Collection<? extends V> arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = target.addAll(helper.unproxy(arg0));
			if (result)
			{
				this.markDirty();
			}
		}
		return result;
	}

	@Override
	public void clear()
	{
		if (target != null)
		{
			if (this.target.size() > 0)
			{
				this.markDirty();
			}
			this.target.clear();
		}
	}

	@Override
	public boolean contains(Object arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = this.target.contains(helper.unproxy(arg0));
		}
		return result;
	}

	@Override
	public boolean containsAll(Collection<?> arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = this.target.containsAll(helper.unproxy(arg0));
		}
		return result;
	}

	@Override
	public boolean isEmpty()
	{
		boolean result = true;
		if (target != null)
		{
			result = this.target.isEmpty();
		}
		return result;
	}

	@Override
	public Iterator<V> iterator()
	{
		Iterator<V> result = null;
		if (target != null)
		{
			result = builder(this.target.iterator()) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta(propertyMeta) //
					.helper(helper) //
					.build();
		}
		return result;
	}

	@Override
	public boolean remove(Object arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = this.target.remove(helper.unproxy(arg0));
			if (result)
			{
				this.markDirty();
			}
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = this.target.removeAll(helper.unproxy(arg0));
			if (result)
			{
				this.markDirty();
			}
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> arg0)
	{
		boolean result = false;
		if (target != null)
		{
			result = this.target.retainAll(helper.unproxy(arg0));
			if (result)
			{
				this.markDirty();
			}
		}
		return result;
	}

	@Override
	public int size()
	{
		int result = 0;
		if (target != null)
		{
			result = this.target.size();
		}
		return result;
	}

	@Override
	public Object[] toArray()
	{
		Object[] result = null;
		if (target != null)
		{
			if (isJoin())
			{
				Object[] array = new Object[this.target.size()];
				int i = 0;
				for (V joinEntity : this.target)
				{
					array[i] = helper.buildProxy(joinEntity, joinMeta());
					i++;
				}

				result = array;
			}
			else
			{

				result = this.target.toArray();
			}
		}
		return result;
	}

	@Override
	public <T> T[] toArray(T[] arg0)
	{
		T[] result = null;
		if (target != null)
		{
			if (isJoin())
			{
				T[] array = this.target.toArray(arg0);

				for (int i = 0; i < array.length; i++)
				{
					array[i] = helper.buildProxy(array[i], joinMeta());
				}
				result = array;
			}
			else
			{

				result = this.target.toArray(arg0);
			}
		}
		return result;
	}

	public Collection<V> getTarget()
	{
		Collection<V> result = null;
		if (target != null)
		{
			result = this.target;
		}
		return result;
	}
}
