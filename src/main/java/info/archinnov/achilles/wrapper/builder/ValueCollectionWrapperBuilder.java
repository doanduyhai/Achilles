package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.wrapper.ValueCollectionWrapper;

import java.util.Collection;

/**
 * ValueCollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValueCollectionWrapperBuilder<ID, V> extends
		AbstractWrapperBuilder<ID, ValueCollectionWrapperBuilder<ID, V>, Void, V>
{
	private Collection<V> target;

	public ValueCollectionWrapperBuilder(PersistenceContext<ID> context, Collection<V> target) {
		super.context = context;
		this.target = target;
	}

	public static <ID, V> ValueCollectionWrapperBuilder<ID, V> builder(
			PersistenceContext<ID> context, Collection<V> target)
	{
		return new ValueCollectionWrapperBuilder<ID, V>(context, target);
	}

	public ValueCollectionWrapper<ID, V> build()
	{
		ValueCollectionWrapper<ID, V> valueCollectionWrapper = new ValueCollectionWrapper<ID, V>(
				this.target);
		super.build(valueCollectionWrapper);
		return valueCollectionWrapper;
	}

}
