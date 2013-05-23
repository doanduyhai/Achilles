package info.archinnov.achilles.proxy.wrapper;

import java.util.Collection;

/**
 * AchillesValueCollectionWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesValueCollectionWrapper<V> extends AchillesCollectionWrapper<V>
{

	public AchillesValueCollectionWrapper(Collection<V> target) {
		super(target);
	}

	@Override
	public boolean add(V arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for a key set");
	}

	@Override
	public boolean addAll(Collection<? extends V> arg0)
	{
		throw new UnsupportedOperationException("This method is not supported for a key set");
	}
}
