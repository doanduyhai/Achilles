package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.EntryIteratorWrapper;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * EntryIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntryIteratorWrapperBuilder<ID, K, V> extends
		AbstractWrapperBuilder<ID, EntryIteratorWrapperBuilder<ID, K, V>, K, V>
{
	private Iterator<Entry<K, V>> target;

	public static <ID, K, V> EntryIteratorWrapperBuilder<ID, K, V> builder(
			AchillesPersistenceContext<ID> context, Iterator<Entry<K, V>> target)
	{
		return new EntryIteratorWrapperBuilder<ID, K, V>(context, target);
	}

	public EntryIteratorWrapperBuilder(AchillesPersistenceContext<ID> context,
			Iterator<Entry<K, V>> target)
	{
		super.context = context;
		this.target = target;
	}

	public EntryIteratorWrapper<ID, K, V> build()
	{
		EntryIteratorWrapper<ID, K, V> iteratorWrapper = new EntryIteratorWrapper<ID, K, V>(
				this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
