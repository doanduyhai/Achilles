package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.wrapper.CollectionWrapper;

import java.util.Collection;

/**
 * CollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CollectionWrapperBuilder<ID, V> extends
		AbstractWrapperBuilder<ID, CollectionWrapperBuilder<ID, V>, Void, V>
{
	private Collection<V> target;

	public static <ID, V> CollectionWrapperBuilder<ID, V> builder(ThriftPersistenceContext<ID> context,
			Collection<V> target)
	{
		return new CollectionWrapperBuilder<ID, V>(context, target);
	}

	public CollectionWrapperBuilder(ThriftPersistenceContext<ID> context, Collection<V> target) {
		super.context = context;
		this.target = target;
	}

	public CollectionWrapper<ID, V> build()
	{
		CollectionWrapper<ID, V> collectionWrapper = new CollectionWrapper<ID, V>(this.target);
		super.build(collectionWrapper);
		return collectionWrapper;
	}

}
