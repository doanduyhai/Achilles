package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesIteratorWrapper;

import java.util.Iterator;

/**
 * AchillesIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesIteratorWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesIteratorWrapperBuilder<V>, Void, V>
{
	private Iterator<V> target;

	public static <V> AchillesIteratorWrapperBuilder<V> builder(AchillesPersistenceContext context,
			Iterator<V> target)
	{
		return new AchillesIteratorWrapperBuilder<V>(context, target);
	}

	public AchillesIteratorWrapperBuilder(AchillesPersistenceContext context, Iterator<V> target) {
		super.context = context;
		this.target = target;
	}

	public AchillesIteratorWrapper<V> build()
	{
		AchillesIteratorWrapper<V> iteratorWrapper = new AchillesIteratorWrapper<V>(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
