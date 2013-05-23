package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesSetWrapper;

import java.util.Set;

/**
 * AchillesSetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesSetWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesSetWrapperBuilder<V>, Void, V>
{
	private Set<V> target;

	public static <ID, V> AchillesSetWrapperBuilder<V> builder(AchillesPersistenceContext context,
			Set<V> target)
	{
		return new AchillesSetWrapperBuilder<V>(context, target);
	}

	public AchillesSetWrapperBuilder(AchillesPersistenceContext context, Set<V> target) {
		super.context = context;
		this.target = target;
	}

	public AchillesSetWrapper<V> build()
	{
		AchillesSetWrapper<V> setWrapper = new AchillesSetWrapper<V>(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
