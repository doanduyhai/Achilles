package fr.doan.achilles.wrapper;

import java.util.ListIterator;

public class ListIteratorProxy<E> extends AbstractProxy<E> implements ListIterator<E>
{

	private final ListIterator<E> target;

	public ListIteratorProxy(ListIterator<E> target) {
		this.target = target;
	}

	@Override
	public void add(E e)
	{
		this.target.add(e);
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
	public E next()
	{
		return this.target.next();
	}

	@Override
	public int nextIndex()
	{
		return this.target.nextIndex();
	}

	@Override
	public E previous()
	{
		return this.target.previous();
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
	public void set(E e)
	{
		this.target.set(e);
		this.markDirty();

	}

}
