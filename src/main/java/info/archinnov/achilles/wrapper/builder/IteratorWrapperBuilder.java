package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.wrapper.IteratorWrapper;

import java.util.Iterator;

/**
 * IteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapperBuilder<ID, V> extends
		AbstractWrapperBuilder<ID, IteratorWrapperBuilder<ID, V>, Void, V>
{
	private Iterator<V> target;

	public static <ID, V> IteratorWrapperBuilder<ID, V> builder(PersistenceContext<ID> context,
			Iterator<V> target)
	{
		return new IteratorWrapperBuilder<ID, V>(context, target);
	}

	public IteratorWrapperBuilder(PersistenceContext<ID> context, Iterator<V> target) {
		super.context = context;
		this.target = target;
	}

	public IteratorWrapper<ID, V> build()
	{
		IteratorWrapper<ID, V> iteratorWrapper = new IteratorWrapper<ID, V>(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
