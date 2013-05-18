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
public class EntryIteratorWrapperBuilder<K, V> extends
		AbstractWrapperBuilder<EntryIteratorWrapperBuilder<K, V>, K, V>
{
	private Iterator<Entry<K, V>> target;

	public static <K, V> EntryIteratorWrapperBuilder<K, V> builder(
			AchillesPersistenceContext context, Iterator<Entry<K, V>> target)
	{
		return new EntryIteratorWrapperBuilder<K, V>(context, target);
	}

	public EntryIteratorWrapperBuilder(AchillesPersistenceContext context,
			Iterator<Entry<K, V>> target)
	{
		super.context = context;
		this.target = target;
	}

	public EntryIteratorWrapper<K, V> build()
	{
		EntryIteratorWrapper<K, V> iteratorWrapper = new EntryIteratorWrapper<K, V>(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
