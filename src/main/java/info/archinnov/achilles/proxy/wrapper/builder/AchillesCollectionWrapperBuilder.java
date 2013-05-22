package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesCollectionWrapper;

import java.util.Collection;

/**
 * AchillesCollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesCollectionWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesCollectionWrapperBuilder<V>, Void, V>
{
	private Collection<V> target;

	public static <V> AchillesCollectionWrapperBuilder<V> builder(
			AchillesPersistenceContext context, Collection<V> target)
	{
		return new AchillesCollectionWrapperBuilder<V>(context, target);
	}

	public AchillesCollectionWrapperBuilder(AchillesPersistenceContext context, Collection<V> target)
	{
		super.context = context;
		this.target = target;
	}

	public AchillesCollectionWrapper<V> build()
	{
		AchillesCollectionWrapper<V> collectionWrapper = new AchillesCollectionWrapper<V>(
				this.target);
		super.build(collectionWrapper);
		return collectionWrapper;
	}

}
