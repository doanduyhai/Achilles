package fr.doan.achilles.wrapper;

import java.util.Collection;
import java.util.Set;

public class KeySetProxy<K> extends SetProxy<K>
{

	public KeySetProxy(Set<K> target) {
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
