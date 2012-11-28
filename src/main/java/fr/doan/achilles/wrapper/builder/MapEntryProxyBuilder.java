package fr.doan.achilles.wrapper.builder;

import java.util.Map;

import fr.doan.achilles.wrapper.MapEntryProxy;

public class MapEntryProxyBuilder<K, V> extends AbstractProxyBuilder<MapEntryProxyBuilder<K, V>, V>
{
	private final Map.Entry<K, V> target;

	public MapEntryProxyBuilder(Map.Entry<K, V> target) {
		this.target = target;
	}

	public static <K, V> MapEntryProxyBuilder<K, V> builder(Map.Entry<K, V> target)
	{
		return new MapEntryProxyBuilder<K, V>(target);
	}

	public MapEntryProxy<K, V> build()
	{
		MapEntryProxy<K, V> proxy = new MapEntryProxy<K, V>(this.target);
		super.build(proxy);
		return proxy;
	}

}
