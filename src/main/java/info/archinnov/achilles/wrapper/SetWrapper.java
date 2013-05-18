package info.archinnov.achilles.wrapper;

import java.util.Set;

/**
 * SetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapper<V> extends CollectionWrapper<V> implements Set<V>
{

	public SetWrapper(Set<V> target) {
		super(target);
	}

	@Override
	public Set<V> getTarget()
	{
		return ((Set<V>) super.target);
	}
}
