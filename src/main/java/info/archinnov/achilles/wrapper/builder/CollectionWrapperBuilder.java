package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.wrapper.CollectionWrapper;

import java.util.Collection;


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
