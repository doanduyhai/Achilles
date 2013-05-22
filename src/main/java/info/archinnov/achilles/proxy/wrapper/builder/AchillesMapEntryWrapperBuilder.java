package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesMapEntryWrapper;

import java.util.Map;

/**
 * AchillesMapEntryWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesMapEntryWrapperBuilder<K, V> extends
		AchillesAbstractWrapperBuilder<AchillesMapEntryWrapperBuilder<K, V>, K, V>
{
	private final Map.Entry<K, V> target;

	public AchillesMapEntryWrapperBuilder(AchillesPersistenceContext context, Map.Entry<K, V> target)
	{
		super.context = context;
		this.target = target;
	}

	public static <K, V> AchillesMapEntryWrapperBuilder<K, V> builder(
			AchillesPersistenceContext context, Map.Entry<K, V> target)
	{
		return new AchillesMapEntryWrapperBuilder<K, V>(context, target);
	}

	public AchillesMapEntryWrapper<K, V> build()
	{
		AchillesMapEntryWrapper<K, V> wrapper = new AchillesMapEntryWrapper<K, V>(this.target);
		super.build(wrapper);
		return wrapper;
	}

}
