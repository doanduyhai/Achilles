package fr.doan.achilles.wrapper.builder;

import java.util.List;

import fr.doan.achilles.wrapper.ListProxy;

public class ListProxyBuilder<E> extends AbstractProxyBuilder<ListProxyBuilder<E>, E>
{
	private List<E> target;

	public static <E> ListProxyBuilder<E> builder(List<E> target)
	{
		return new ListProxyBuilder<E>(target);
	}

	public ListProxyBuilder(List<E> target) {
		this.target = target;
	}

	public ListProxy<E> build()
	{
		ListProxy<E> listProxy = new ListProxy<E>(this.target);
		super.build(listProxy);
		return listProxy;
	}

}
