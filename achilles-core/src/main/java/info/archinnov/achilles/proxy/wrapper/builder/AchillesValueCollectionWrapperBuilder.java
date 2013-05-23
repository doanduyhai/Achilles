package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesValueCollectionWrapper;

import java.util.Collection;

/**
 * AchillesValueCollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesValueCollectionWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesValueCollectionWrapperBuilder<V>, Void, V>
{
	private Collection<V> target;

	public AchillesValueCollectionWrapperBuilder(AchillesPersistenceContext context,
			Collection<V> target)
	{
		super.context = context;
		this.target = target;
	}

	public static <V> AchillesValueCollectionWrapperBuilder<V> builder(
			AchillesPersistenceContext context, Collection<V> target)
	{
		return new AchillesValueCollectionWrapperBuilder<V>(context, target);
	}

	public AchillesValueCollectionWrapper<V> build()
	{
		AchillesValueCollectionWrapper<V> valueCollectionWrapper = new AchillesValueCollectionWrapper<V>(
				this.target);
		super.build(valueCollectionWrapper);
		return valueCollectionWrapper;
	}

}
