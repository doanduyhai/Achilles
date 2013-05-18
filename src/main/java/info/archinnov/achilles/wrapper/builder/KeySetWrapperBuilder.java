package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.AbstractWrapper;
import info.archinnov.achilles.wrapper.KeySetWrapper;

import java.util.Set;

/**
 * KeySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeySetWrapperBuilder<K> extends
		AbstractWrapperBuilder<KeySetWrapperBuilder<K>, K, Void>
{
	private Set<K> target;

	public KeySetWrapperBuilder(AchillesPersistenceContext context, Set<K> target) {
		super.context = context;
		this.target = target;
	}

	public static <K> KeySetWrapperBuilder<K> builder(AchillesPersistenceContext context,
			Set<K> target)
	{
		return new KeySetWrapperBuilder<K>(context, target);
	}

	public KeySetWrapper<K> build()
	{
		KeySetWrapper<K> keySetWrapper = new KeySetWrapper<K>(this.target);
		super.build((AbstractWrapper<K, Void>) keySetWrapper);
		return keySetWrapper;
	}

}
