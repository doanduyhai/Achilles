package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AbstractWrapper;
import info.archinnov.achilles.proxy.wrapper.KeySetWrapper;

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

	@SuppressWarnings("unchecked")
	public KeySetWrapper<K> build()
	{
		KeySetWrapper<K> keySetWrapper = new KeySetWrapper<K>(this.target);
		super.build((AbstractWrapper<K, Void>) keySetWrapper);
		return keySetWrapper;
	}

}
