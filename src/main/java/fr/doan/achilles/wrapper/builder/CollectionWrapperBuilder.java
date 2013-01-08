package fr.doan.achilles.wrapper.builder;

import java.util.Collection;

import fr.doan.achilles.wrapper.CollectionWrapper;

/**
 * CollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CollectionWrapperBuilder<V> extends
		AbstractWrapperBuilder<CollectionWrapperBuilder<V>, Void, V>
{
	private Collection<V> target;

	public static <V> CollectionWrapperBuilder<V> builder(Collection<V> target)
	{
		return new CollectionWrapperBuilder<V>(target);
	}

	public CollectionWrapperBuilder(Collection<V> target) {
		this.target = target;
	}

	public CollectionWrapper<V> build()
	{
		CollectionWrapper<V> collectionWrapper = new CollectionWrapper<V>(this.target);
		super.build(collectionWrapper);
		return collectionWrapper;
	}

}
