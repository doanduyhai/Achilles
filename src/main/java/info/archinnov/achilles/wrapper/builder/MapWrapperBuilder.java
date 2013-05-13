package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.MapWrapper;

import java.util.Map;

/**
 * MapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapWrapperBuilder<ID, K, V> extends
		AbstractWrapperBuilder<ID, MapWrapperBuilder<ID, K, V>, K, V>
{
	private Map<K, V> target;

	public static <ID, K, V> MapWrapperBuilder<ID, K, V> builder(
			AchillesPersistenceContext<ID> context, Map<K, V> target)
	{
		return new MapWrapperBuilder<ID, K, V>(context, target);
	}

	public MapWrapperBuilder(AchillesPersistenceContext<ID> context, Map<K, V> target) {
		super.context = context;
		this.target = target;
	}

	public MapWrapper<ID, K, V> build()
	{
		MapWrapper<ID, K, V> mapWrapper = new MapWrapper<ID, K, V>(this.target);
		super.build(mapWrapper);
		return mapWrapper;
	}
}
