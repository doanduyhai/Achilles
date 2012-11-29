package fr.doan.achilles.wrapper.builder;

import java.util.List;

import fr.doan.achilles.wrapper.ListWrapper;

public class ListWrapperBuilder<E> extends AbstractWrapperBuilder<ListWrapperBuilder<E>, E>
{
	private List<E> target;

	public static <E> ListWrapperBuilder<E> builder(List<E> target)
	{
		return new ListWrapperBuilder<E>(target);
	}

	public ListWrapperBuilder(List<E> target) {
		this.target = target;
	}

	public ListWrapper<E> build()
	{
		ListWrapper<E> listWrapper = new ListWrapper<E>(this.target);
		super.build(listWrapper);
		return listWrapper;
	}

}
