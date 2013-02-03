package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.wrapper.MapWrapper;

import java.util.Map;


/**
 * MapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
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
