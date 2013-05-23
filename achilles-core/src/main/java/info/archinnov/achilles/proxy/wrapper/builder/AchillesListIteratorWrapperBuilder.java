package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesListIteratorWrapper;

import java.util.ListIterator;

/**
 * AchillesListIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesListIteratorWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesListIteratorWrapperBuilder<V>, Void, V>
{
	private ListIterator<V> target;

	public static <V> AchillesListIteratorWrapperBuilder<V> builder(
			AchillesPersistenceContext context, ListIterator<V> target)
	{
		return new AchillesListIteratorWrapperBuilder<V>(context, target);
	}

	public AchillesListIteratorWrapperBuilder(AchillesPersistenceContext context,
			ListIterator<V> target)
	{
		super.context = context;
		this.target = target;
	}

	public AchillesListIteratorWrapper<V> build()
	{
		AchillesListIteratorWrapper<V> listIteratorWrapper = new AchillesListIteratorWrapper<V>(
				this.target);
		super.build(listIteratorWrapper);
		return listIteratorWrapper;
	}

}
