package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.wrapper.builder.EntryIteratorWrapperBuilder.builder;
import static info.archinnov.achilles.wrapper.builder.MapEntryWrapperBuilder.builder;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * EntrySetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapper<K, V> extends AbstractWrapper<K, V> implements Set<Entry<K, V>>
{

	private Set<Entry<K, V>> target;

	public EntrySetWrapper(Set<Entry<K, V>> target) {
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
		if (target != null)
		{
			this.target.clear();
			this.markDirty();
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
		boolean result = false;
		if (target != null)
		{
			result = this.target.isEmpty();
		}
		return result;
	}

	@Override
	public Iterator<Entry<K, V>> iterator()
	{
		Iterator<Entry<K, V>> result = null;
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
				Object[] array = new MapEntryWrapper[this.target.size()];
				int i = 0;
				for (Map.Entry<K, V> entry : this.target)
				{
					array[i] = builder(entry) //
							.dirtyMap(dirtyMap) //
							.setter(setter) //
							.propertyMeta(propertyMeta) //
							.helper(helper) //
							.build();
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

}
