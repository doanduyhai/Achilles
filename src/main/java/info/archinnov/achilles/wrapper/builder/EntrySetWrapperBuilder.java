package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.wrapper.EntrySetWrapper;

import java.util.Map.Entry;
import java.util.Set;


/**
 * EntrySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapperBuilder<K, V> extends
		AbstractWrapperBuilder<EntrySetWrapperBuilder<K, V>, K, V>
{
	private Set<Entry<K, V>> target;

	public static <K, V> EntrySetWrapperBuilder<K, V> builder(Set<Entry<K, V>> target)
	{
		return new EntrySetWrapperBuilder<K, V>(target);
	}

	public EntrySetWrapperBuilder(Set<Entry<K, V>> target) {
		this.target = target;
	}

	public EntrySetWrapper<K, V> build()
	{
		EntrySetWrapper<K, V> entrySetWrapper = new EntrySetWrapper<K, V>(this.target);
		super.build(entrySetWrapper);
		return entrySetWrapper;
	}
}
