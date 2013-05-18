package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.MapEntryWrapper;

import java.util.Map;

/**
 * MapEntryWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapEntryWrapperBuilder<K, V> extends
		AbstractWrapperBuilder<MapEntryWrapperBuilder<K, V>, K, V>
{
	private final Map.Entry<K, V> target;

	public MapEntryWrapperBuilder(AchillesPersistenceContext context, Map.Entry<K, V> target) {
		super.context = context;
		this.target = target;
	}

	public static <K, V> MapEntryWrapperBuilder<K, V> builder(AchillesPersistenceContext context,
			Map.Entry<K, V> target)
	{
		return new MapEntryWrapperBuilder<K, V>(context, target);
	}

	public MapEntryWrapper<K, V> build()
	{
		MapEntryWrapper<K, V> wrapper = new MapEntryWrapper<K, V>(this.target);
		super.build(wrapper);
		return wrapper;
	}

}
