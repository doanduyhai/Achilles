package fr.doan.achilles.wrapper.builder;

import java.util.Iterator;

import fr.doan.achilles.wrapper.IteratorWrapper;

public class IteratorWrapperBuilder<V> extends
		AbstractWrapperBuilder<IteratorWrapperBuilder<V>, Void, V>
{
	private Iterator<V> target;

	public static <V> IteratorWrapperBuilder<V> builder(Iterator<V> target)
	{
		return new IteratorWrapperBuilder<V>(target);
	}

	public IteratorWrapperBuilder(Iterator<V> target) {
		this.target = target;
	}

	public IteratorWrapper<V> build()
	{
		IteratorWrapper<V> iteratorWrapper = new IteratorWrapper<V>(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
