package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.AchillesListWrapper;

import java.util.List;

/**
 * AchillesListWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesListWrapperBuilder<V> extends
		AchillesAbstractWrapperBuilder<AchillesListWrapperBuilder<V>, Void, V>
{
	private List<V> target;

	public static <V> AchillesListWrapperBuilder<V> builder(AchillesPersistenceContext context,
			List<V> target)
	{
		return new AchillesListWrapperBuilder<V>(context, target);
	}

	public AchillesListWrapperBuilder(AchillesPersistenceContext context, List<V> target) {
		super.context = context;
		this.target = target;
	}

	public AchillesListWrapper<V> build()
	{
		AchillesListWrapper<V> listWrapper = new AchillesListWrapper<V>(this.target);
		super.build(listWrapper);
		return listWrapper;
	}

}
