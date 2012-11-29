package fr.doan.achilles.wrapper.builder;

import java.util.Collection;

import fr.doan.achilles.wrapper.CollectionWrapper;

public class CollectionWrapperBuilder<E> extends AbstractWrapperBuilder<CollectionWrapperBuilder<E>, E>
{
	private Collection<E> target;

	public static <E> CollectionWrapperBuilder<E> builder(Collection<E> target)
	{
		return new CollectionWrapperBuilder<E>(target);
	}

	public CollectionWrapperBuilder(Collection<E> target) {
		this.target = target;
	}

	public CollectionWrapper<E> build()
	{
		CollectionWrapper<E> collectionWrapper = new CollectionWrapper<E>(this.target);
		super.build(collectionWrapper);
		return collectionWrapper;
	}

}
