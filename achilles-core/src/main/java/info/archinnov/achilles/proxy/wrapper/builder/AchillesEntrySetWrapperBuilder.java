package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesEntrySetWrapper;

import java.util.Map.Entry;
import java.util.Set;

/**
 * AchillesEntrySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntrySetWrapperBuilder<K, V> extends
		AchillesAbstractWrapperBuilder<AchillesEntrySetWrapperBuilder<K, V>, K, V>
{
	private Set<Entry<K, V>> target;

	public static <K, V> AchillesEntrySetWrapperBuilder<K, V> builder(
			AchillesPersistenceContext context, Set<Entry<K, V>> target)
	{
		return new AchillesEntrySetWrapperBuilder<K, V>(context, target);
	}

	public AchillesEntrySetWrapperBuilder(AchillesPersistenceContext context,
			Set<Entry<K, V>> target)
	{
		super.context = context;
		this.target = target;
	}

	public AchillesEntrySetWrapper<K, V> build()
	{
		AchillesEntrySetWrapper<K, V> entrySetWrapper = new AchillesEntrySetWrapper<K, V>(
				this.target);
		super.build(entrySetWrapper);
		return entrySetWrapper;
	}
}
