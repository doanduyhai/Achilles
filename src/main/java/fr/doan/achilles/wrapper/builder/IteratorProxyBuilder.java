package fr.doan.achilles.wrapper.builder;

import java.util.Iterator;

import fr.doan.achilles.wrapper.IteratorProxy;

public class IteratorProxyBuilder<E> extends AbstractProxyBuilder<IteratorProxyBuilder<E>, E>
{
	private Iterator<E> target;

	public static <E> IteratorProxyBuilder<E> builder(Iterator<E> target)
	{
		return new IteratorProxyBuilder<E>(target);
	}

	public IteratorProxyBuilder(Iterator<E> target) {
		this.target = target;
	}

	public IteratorProxy<E> build()
	{
		IteratorProxy<E> iteratorProxy = new IteratorProxy<E>(this.target);
		super.build(iteratorProxy);
		return iteratorProxy;
	}
}
