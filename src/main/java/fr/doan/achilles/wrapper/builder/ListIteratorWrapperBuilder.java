package fr.doan.achilles.wrapper.builder;

import java.util.ListIterator;

import fr.doan.achilles.wrapper.ListIteratorWrapper;

public class ListIteratorWrapperBuilder<E> extends AbstractWrapperBuilder<ListIteratorWrapperBuilder<E>, E>
{
	private ListIterator<E> target;

	public static <E> ListIteratorWrapperBuilder<E> builder(ListIterator<E> target)
	{
		return new ListIteratorWrapperBuilder<E>(target);
	}

	public ListIteratorWrapperBuilder(ListIterator<E> target) {
		this.target = target;
	}

	public ListIteratorWrapper<E> build()
	{
		ListIteratorWrapper<E> listIteratorWrapper = new ListIteratorWrapper<E>(this.target);
		super.build(listIteratorWrapper);
		return listIteratorWrapper;
	}

}
