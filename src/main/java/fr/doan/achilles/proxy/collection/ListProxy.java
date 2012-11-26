package fr.doan.achilles.proxy.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListProxy<E> extends CollectionProxy<E> implements List<E>
{

	public ListProxy(List<E> target) {
		super(target);
	}

	@Override
	public void add(int arg0, E arg1)
	{
		((List<E>) super.target).add(arg0, arg1);
		super.markDirty();
	}

	@Override
	public boolean addAll(int arg0, Collection<? extends E> arg1)
	{
		boolean result = ((List<E>) super.target).addAll(arg0, arg1);
		if (result)
		{
			super.markDirty();
		}
		return result;
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
	public E get(int arg0)
	{
		return ((List<E>) super.target).get(arg0);
	}

	@Override
	public int indexOf(Object arg0)
	{
		return ((List<E>) super.target).indexOf(arg0);
	}

	@Override
	public Iterator<E> iterator()
	{
		return this.target.iterator();
	}

	@Override
	public int lastIndexOf(Object arg0)
	{
		return ((List<E>) super.target).lastIndexOf(arg0);
	}

	@Override
	public ListIterator<E> listIterator()
	{
		return ((List<E>) super.target).listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int arg0)
	{
		return ((List<E>) super.target).listIterator(arg0);
	}

	@Override
	public E remove(int arg0)
	{
		E result = ((List<E>) super.target).remove(arg0);
		super.markDirty();
		return result;
	}

	@Override
	public E set(int arg0, E arg1)
	{
		E result = ((List<E>) super.target).set(arg0, arg1);
		super.markDirty();
		return result;
	}

	@Override
	public List<E> subList(int arg0, int arg1)
	{
		return ((List<E>) super.target).subList(arg0, arg1);
	}

	public List<E> getTarget()
	{
		return ((List<E>) super.target);
	}

}
