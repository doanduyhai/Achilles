package info.archinnov.achilles.proxy.wrapper;

import java.util.Collection;
import java.util.Set;

/**
 * AchillesKeySetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesKeySetWrapper<K> extends AchillesSetWrapper<K>
{
	public AchillesKeySetWrapper(Set<K> target) {
		super(target);
	}

	@Override
	public boolean add(K arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for a key set");
	}

	@Override
	public boolean addAll(Collection<? extends K> arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for a key set");
	}
}
