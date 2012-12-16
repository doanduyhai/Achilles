package fr.doan.achilles.wrapper.builder;

import java.util.Map;

import fr.doan.achilles.wrapper.MapWrapper;

public class MapWrapperBuilder<K, V> extends AbstractWrapperBuilder<MapWrapperBuilder<K, V>, K, V>
{
	private Map<K, V> target;

	public static <K, V> MapWrapperBuilder<K, V> builder(Map<K, V> target)
	{
		return new MapWrapperBuilder<K, V>(target);
	}

	public MapWrapperBuilder(Map<K, V> target) {
		this.target = target;
	}

	public MapWrapper<K, V> build()
	{
		MapWrapper<K, V> mapWrapper = new MapWrapper<K, V>(this.target);
		super.build(mapWrapper);
		return mapWrapper;
	}
}
