package fr.doan.achilles.wrapper.builder;

import java.util.Collection;

import fr.doan.achilles.wrapper.CollectionProxy;

public class CollectionProxyBuilder<E> extends AbstractProxyBuilder<CollectionProxyBuilder<E>, E>
{
	private Collection<E> target;

	public static <E> CollectionProxyBuilder<E> builder(Collection<E> target)
	{
		return new CollectionProxyBuilder<E>(target);
	}

	public CollectionProxyBuilder(Collection<E> target) {
		this.target = target;
	}

	public CollectionProxy<E> build()
	{
		CollectionProxy<E> collectionProxy = new CollectionProxy<E>(this.target);
		super.build(collectionProxy);
		return collectionProxy;
	}

}
