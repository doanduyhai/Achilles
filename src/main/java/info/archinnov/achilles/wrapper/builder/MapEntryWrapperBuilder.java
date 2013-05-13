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
public class MapEntryWrapperBuilder<ID, K, V> extends
		AbstractWrapperBuilder<ID, MapEntryWrapperBuilder<ID, K, V>, K, V>
{
	private final Map.Entry<K, V> target;

	public MapEntryWrapperBuilder(AchillesPersistenceContext<ID> context, Map.Entry<K, V> target) {
		super.context = context;
		this.target = target;
	}

	public static <ID, K, V> MapEntryWrapperBuilder<ID, K, V> builder(
			AchillesPersistenceContext<ID> context, Map.Entry<K, V> target)
	{
		return new MapEntryWrapperBuilder<ID, K, V>(context, target);
	}

	public MapEntryWrapper<ID, K, V> build()
	{
		MapEntryWrapper<ID, K, V> wrapper = new MapEntryWrapper<ID, K, V>(this.target);
		super.build(wrapper);
		return wrapper;
	}

}
