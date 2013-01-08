package fr.doan.achilles.wrapper.builder;

import java.util.ListIterator;

import fr.doan.achilles.wrapper.ListIteratorWrapper;

/**
 * ListIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListIteratorWrapperBuilder<V> extends
		AbstractWrapperBuilder<ListIteratorWrapperBuilder<V>, Void, V>
{
	private ListIterator<V> target;

	public static <V> ListIteratorWrapperBuilder<V> builder(ListIterator<V> target)
	{
		return new ListIteratorWrapperBuilder<V>(target);
	}

	public ListIteratorWrapperBuilder(ListIterator<V> target) {
		this.target = target;
	}

	public ListIteratorWrapper<V> build()
	{
		ListIteratorWrapper<V> listIteratorWrapper = new ListIteratorWrapper<V>(this.target);
		super.build(listIteratorWrapper);
		return listIteratorWrapper;
	}

}
