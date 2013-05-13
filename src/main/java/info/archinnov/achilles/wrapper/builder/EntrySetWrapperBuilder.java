package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.EntrySetWrapper;

import java.util.Map.Entry;
import java.util.Set;

/**
 * EntrySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapperBuilder<ID, K, V> extends
		AbstractWrapperBuilder<ID, EntrySetWrapperBuilder<ID, K, V>, K, V>
{
	private Set<Entry<K, V>> target;

	public static <ID, K, V> EntrySetWrapperBuilder<ID, K, V> builder(
			AchillesPersistenceContext<ID> context, Set<Entry<K, V>> target)
	{
		return new EntrySetWrapperBuilder<ID, K, V>(context, target);
	}

	public EntrySetWrapperBuilder(AchillesPersistenceContext<ID> context, Set<Entry<K, V>> target) {
		super.context = context;
		this.target = target;
	}

	public EntrySetWrapper<ID, K, V> build()
	{
		EntrySetWrapper<ID, K, V> entrySetWrapper = new EntrySetWrapper<ID, K, V>(this.target);
		super.build(entrySetWrapper);
		return entrySetWrapper;
	}
}
