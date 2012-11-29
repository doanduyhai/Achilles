package fr.doan.achilles.wrapper.builder;

import java.util.Map;

import fr.doan.achilles.wrapper.MapEntryWrapper;

public class MapEntryWrapperBuilder<K, V> extends AbstractWrapperBuilder<MapEntryWrapperBuilder<K, V>, V>
{
	private final Map.Entry<K, V> target;

	public MapEntryWrapperBuilder(Map.Entry<K, V> target) {
		this.target = target;
	}

	public static <K, V> MapEntryWrapperBuilder<K, V> builder(Map.Entry<K, V> target)
	{
		return new MapEntryWrapperBuilder<K, V>(target);
	}

	public MapEntryWrapper<K, V> build()
	{
		MapEntryWrapper<K, V> wrapper = new MapEntryWrapper<K, V>(this.target);
		super.build(wrapper);
		return wrapper;
	}

}
