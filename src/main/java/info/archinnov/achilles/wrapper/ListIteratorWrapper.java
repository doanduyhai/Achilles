package info.archinnov.achilles.wrapper;

import java.util.ListIterator;

/**
 * ListIteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListIteratorWrapper<V> extends AbstractWrapper<Void, V> implements ListIterator<V>
{

	private ListIterator<V> target;

	public ListIteratorWrapper(ListIterator<V> target) {
		this.target = target;
	}

	@Override
	public void add(V e)
	{
		this.target.add(helper.unproxy(e));
		this.markDirty();
	}

	@Override
	public boolean hasNext()
	{
		return this.target.hasNext();
	}

	@Override
	public boolean hasPrevious()
	{
		return this.target.hasPrevious();
	}

	@Override
	public V next()
	{
		if (isJoin())
		{
			return helper.buildProxy(this.target.next(), joinMeta());
		}
		else
		{
			return this.target.next();
		}
	}

	@Override
	public int nextIndex()
	{
		return this.target.nextIndex();
	}

	@Override
	public V previous()
	{
		if (isJoin())
		{
			return helper.buildProxy(this.target.previous(), joinMeta());
		}
		else
		{
			return this.target.previous();
		}
	}

	@Override
	public int previousIndex()
	{
		return this.target.previousIndex();
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();

	}

	@Override
	public void set(V e)
	{
		this.target.set(helper.unproxy(e));
		this.markDirty();

	}

}
