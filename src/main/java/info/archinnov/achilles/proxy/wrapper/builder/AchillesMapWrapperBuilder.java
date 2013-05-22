package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesMapWrapper;

import java.util.Map;

/**
 * AchillesMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesMapWrapperBuilder<K, V> extends
		AchillesAbstractWrapperBuilder<AchillesMapWrapperBuilder<K, V>, K, V>
{
	private Map<K, V> target;

	public static <ID, K, V> AchillesMapWrapperBuilder<K, V> builder(
			AchillesPersistenceContext context, Map<K, V> target)
	{
		return new AchillesMapWrapperBuilder<K, V>(context, target);
	}

	public AchillesMapWrapperBuilder(AchillesPersistenceContext context, Map<K, V> target) {
		super.context = context;
		this.target = target;
	}

	public AchillesMapWrapper<K, V> build()
	{
		AchillesMapWrapper<K, V> mapWrapper = new AchillesMapWrapper<K, V>(this.target);
		super.build(mapWrapper);
		return mapWrapper;
	}
}
