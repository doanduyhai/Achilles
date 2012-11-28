package fr.doan.achilles.wrapper.builder;

import java.util.ListIterator;

import fr.doan.achilles.wrapper.ListIteratorProxy;

public class ListIteratorProxyBuilder<E> extends AbstractProxyBuilder<ListIteratorProxyBuilder<E>, E>
{
	private ListIterator<E> target;

	public static <E> ListIteratorProxyBuilder<E> builder(ListIterator<E> target)
	{
		return new ListIteratorProxyBuilder<E>(target);
	}

	public ListIteratorProxyBuilder(ListIterator<E> target) {
		this.target = target;
	}

	public ListIteratorProxy<E> build()
	{
		ListIteratorProxy<E> listIteratorProxy = new ListIteratorProxy<E>(this.target);
		super.build(listIteratorProxy);
		return listIteratorProxy;
	}

}
