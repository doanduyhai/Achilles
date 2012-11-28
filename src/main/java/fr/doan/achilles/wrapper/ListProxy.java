package fr.doan.achilles.wrapper;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import fr.doan.achilles.wrapper.builder.ListIteratorProxyBuilder;
import fr.doan.achilles.wrapper.builder.ListProxyBuilder;

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
	public int lastIndexOf(Object arg0)
	{
		return ((List<E>) super.target).lastIndexOf(arg0);
	}

	@Override
	public ListIterator<E> listIterator()
	{
		ListIterator<E> target = ((List<E>) super.target).listIterator();

		return ListIteratorProxyBuilder.builder(target).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();
	}

	@Override
	public ListIterator<E> listIterator(int arg0)
	{
		ListIterator<E> target = ((List<E>) super.target).listIterator(arg0);

		return ListIteratorProxyBuilder.builder(target).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();
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
		List<E> target = ((List<E>) super.target).subList(arg0, arg1);

		return ListProxyBuilder.builder(target).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();
	}

	@Override
	public List<E> getTarget()
	{
		return ((List<E>) super.target);
	}

}
