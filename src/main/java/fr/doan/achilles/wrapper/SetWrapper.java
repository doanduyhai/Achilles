package fr.doan.achilles.wrapper;

import java.util.Set;

/**
 * SetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapper<E> extends CollectionWrapper<E> implements Set<E>
{

	public SetWrapper(Set<E> target) {
		super(target);
	}

	@Override
	public Set<E> getTarget()
	{
		return ((Set<E>) super.target);
	}
}
