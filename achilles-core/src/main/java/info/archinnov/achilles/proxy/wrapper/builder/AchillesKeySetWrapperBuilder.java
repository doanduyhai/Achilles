package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesAbstractWrapper;
import info.archinnov.achilles.proxy.wrapper.AchillesKeySetWrapper;

import java.util.Set;

/**
 * AchillesKeySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesKeySetWrapperBuilder<K> extends
		AchillesAbstractWrapperBuilder<AchillesKeySetWrapperBuilder<K>, K, Void>
{
	private Set<K> target;

	public AchillesKeySetWrapperBuilder(AchillesPersistenceContext context, Set<K> target) {
		super.context = context;
		this.target = target;
	}

	public static <K> AchillesKeySetWrapperBuilder<K> builder(AchillesPersistenceContext context,
			Set<K> target)
	{
		return new AchillesKeySetWrapperBuilder<K>(context, target);
	}

	public AchillesKeySetWrapper<K> build()
	{
		AchillesKeySetWrapper<K> keySetWrapper = new AchillesKeySetWrapper<K>(this.target);
		super.build((AchillesAbstractWrapper<K, Void>) keySetWrapper);
		return keySetWrapper;
	}

}
