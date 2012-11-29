package fr.doan.achilles.wrapper.builder;

import java.util.Iterator;
import java.util.Map.Entry;

import fr.doan.achilles.wrapper.EntryIteratorProxy;

public class EntryIteratorProxyBuilder<K, V> extends AbstractProxyBuilder<EntryIteratorProxyBuilder<K, V>, V>
{
	private Iterator<Entry<K, V>> target;

	public static <K, V> EntryIteratorProxyBuilder<K, V> builder(Iterator<Entry<K, V>> target)
	{
		return new EntryIteratorProxyBuilder<K, V>(target);
	}

	public EntryIteratorProxyBuilder(Iterator<Entry<K, V>> target) {
		this.target = target;
	}

	public EntryIteratorProxy<K, V> build()
	{
		EntryIteratorProxy<K, V> iteratorProxy = new EntryIteratorProxy<K, V>(this.target);
		super.build(iteratorProxy);
		return iteratorProxy;
	}
}
