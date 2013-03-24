package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.wrapper.AbstractWrapper;
import info.archinnov.achilles.wrapper.KeySetWrapper;

import java.util.Set;

/**
 * KeySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeySetWrapperBuilder<ID, K> extends
		AbstractWrapperBuilder<ID, KeySetWrapperBuilder<ID, K>, K, Void>
{
	private Set<K> target;

	public KeySetWrapperBuilder(PersistenceContext<ID> context, Set<K> target) {
		super.context = context;
		this.target = target;
	}

	public static <ID, K> KeySetWrapperBuilder<ID, K> builder(PersistenceContext<ID> context,
			Set<K> target)
	{
		return new KeySetWrapperBuilder<ID, K>(context, target);
	}

	@SuppressWarnings("unchecked")
	public KeySetWrapper<ID, K> build()
	{
		KeySetWrapper<ID, K> keySetWrapper = new KeySetWrapper<ID, K>(this.target);
		super.build((AbstractWrapper<ID, K, Void>) keySetWrapper);
		return keySetWrapper;
	}

}
