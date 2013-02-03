package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.wrapper.IteratorWrapper;

import java.util.Iterator;


/**
 * IteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
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
