package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.ListWrapper;

import java.util.List;

/**
 * ListWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListWrapperBuilder<V> extends AbstractWrapperBuilder<ListWrapperBuilder<V>, Void, V>
{
	private List<V> target;

	public static <V> ListWrapperBuilder<V> builder(AchillesPersistenceContext context,
			List<V> target)
	{
		return new ListWrapperBuilder<V>(context, target);
	}

	public ListWrapperBuilder(AchillesPersistenceContext context, List<V> target) {
		super.context = context;
		this.target = target;
	}

	public ListWrapper<V> build()
	{
		ListWrapper<V> listWrapper = new ListWrapper<V>(this.target);
		super.build(listWrapper);
		return listWrapper;
	}

}
