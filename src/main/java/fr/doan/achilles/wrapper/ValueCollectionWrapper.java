package fr.doan.achilles.wrapper;

import java.util.Collection;

/**
 * ValueCollectionWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValueCollectionWrapper<V> extends CollectionWrapper<V>
{

	public ValueCollectionWrapper(Collection<V> target) {
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
