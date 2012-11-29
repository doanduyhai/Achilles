package fr.doan.achilles.wrapper.builder;

import java.util.Map;

import fr.doan.achilles.wrapper.MapProxy;

public class MapProxyBuilder<K, V> extends AbstractProxyBuilder<MapProxyBuilder<K, V>, V>
{
	private Map<K, V> target;

	public static <K, V> MapProxyBuilder<K, V> builder(Map<K, V> target)
	{
		return new MapProxyBuilder<K, V>(target);
	}

	public MapProxyBuilder(Map<K, V> target) {
		this.target = target;
	}

	public MapProxy<K, V> build()
	{
		MapProxy<K, V> mapProxy = new MapProxy<K, V>(this.target);
		super.build(mapProxy);
		return mapProxy;
	}
}
