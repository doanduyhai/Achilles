package info.archinnov.achilles.proxy.wrapper;

import java.util.Set;

/**
 * AchillesSetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesSetWrapper<V> extends AchillesCollectionWrapper<V> implements Set<V>
{

	public AchillesSetWrapper(Set<V> target) {
		super(target);
	}

	@Override
	public Set<V> getTarget()
	{
		return ((Set<V>) super.target);
	}
}
