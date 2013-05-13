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
public class ListWrapperBuilder<ID, V> extends
		AbstractWrapperBuilder<ID, ListWrapperBuilder<ID, V>, Void, V>
{
	private List<V> target;

	public static <ID, V> ListWrapperBuilder<ID, V> builder(AchillesPersistenceContext<ID> context,
			List<V> target)
	{
		return new ListWrapperBuilder<ID, V>(context, target);
	}

	public ListWrapperBuilder(AchillesPersistenceContext<ID> context, List<V> target) {
		super.context = context;
		this.target = target;
	}

	public ListWrapper<ID, V> build()
	{
		ListWrapper<ID, V> listWrapper = new ListWrapper<ID, V>(this.target);
		super.build(listWrapper);
		return listWrapper;
	}

}
