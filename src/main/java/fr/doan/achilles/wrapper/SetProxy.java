package fr.doan.achilles.wrapper;

import java.util.Set;

public class SetProxy<E> extends CollectionProxy<E> implements Set<E>
{

	public SetProxy(Set<E> target) {
		super(target);
	}

	@Override
	public Set<E> getTarget()
	{
		return ((Set<E>) super.target);
	}
}
