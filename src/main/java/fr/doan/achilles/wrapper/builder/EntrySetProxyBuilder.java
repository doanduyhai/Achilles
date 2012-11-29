package fr.doan.achilles.wrapper.builder;

import java.util.Map.Entry;
import java.util.Set;

import fr.doan.achilles.wrapper.EntrySetProxy;

public class EntrySetProxyBuilder<K, V> extends AbstractProxyBuilder<EntrySetProxyBuilder<K, V>, V>
{
	private Set<Entry<K, V>> target;

	public static <K, V> EntrySetProxyBuilder<K, V> builder(Set<Entry<K, V>> target)
	{
		return new EntrySetProxyBuilder<K, V>(target);
	}

	public EntrySetProxyBuilder(Set<Entry<K, V>> target) {
		this.target = target;
	}

	public EntrySetProxy<K, V> build()
	{
		EntrySetProxy<K, V> entrySetProxy = new EntrySetProxy<K, V>(this.target);
		super.build(entrySetProxy);
		return entrySetProxy;
	}
}
