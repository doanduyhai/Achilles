package fr.doan.achilles.wrapper.builder;

import java.util.Iterator;

import fr.doan.achilles.wrapper.IteratorWrapper;

public class IteratorWrapperBuilder<E> extends AbstractWrapperBuilder<IteratorWrapperBuilder<E>, E>
{
	private Iterator<E> target;

	public static <E> IteratorWrapperBuilder<E> builder(Iterator<E> target)
	{
		return new IteratorWrapperBuilder<E>(target);
	}

	public IteratorWrapperBuilder(Iterator<E> target) {
		this.target = target;
	}

	public IteratorWrapper<E> build()
	{
		IteratorWrapper<E> iteratorWrapper = new IteratorWrapper<E>(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
