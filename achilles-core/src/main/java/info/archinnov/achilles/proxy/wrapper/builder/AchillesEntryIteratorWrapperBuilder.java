package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesEntryIteratorWrapper;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * AchillesEntryIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntryIteratorWrapperBuilder<K, V> extends
		AchillesAbstractWrapperBuilder<AchillesEntryIteratorWrapperBuilder<K, V>, K, V>
{
	private Iterator<Entry<K, V>> target;

	public static <K, V> AchillesEntryIteratorWrapperBuilder<K, V> builder(
			AchillesPersistenceContext context, Iterator<Entry<K, V>> target)
	{
		return new AchillesEntryIteratorWrapperBuilder<K, V>(context, target);
	}

	public AchillesEntryIteratorWrapperBuilder(AchillesPersistenceContext context,
			Iterator<Entry<K, V>> target)
	{
		super.context = context;
		this.target = target;
	}

	public AchillesEntryIteratorWrapper<K, V> build()
	{
		AchillesEntryIteratorWrapper<K, V> iteratorWrapper = new AchillesEntryIteratorWrapper<K, V>(
				this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
