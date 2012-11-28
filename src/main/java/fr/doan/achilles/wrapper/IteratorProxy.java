package fr.doan.achilles.wrapper;

import java.util.Iterator;

public class IteratorProxy<E> extends AbstractProxy<E> implements Iterator<E>
{
	protected Iterator<E> target;

	public IteratorProxy(Iterator<E> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext()
	{
		return this.target.hasNext();
	}

	@Override
	public E next()
	{
		return this.target.next();
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}
}
