package fr.doan.achilles.wrapper.builder;

import java.util.List;

import fr.doan.achilles.wrapper.ListWrapper;

/**
 * ListWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListWrapperBuilder<V> extends AbstractWrapperBuilder<ListWrapperBuilder<V>, Void, V>
{
	private List<V> target;

	public static <V> ListWrapperBuilder<V> builder(List<V> target)
	{
		return new ListWrapperBuilder<V>(target);
	}

	public ListWrapperBuilder(List<V> target) {
		this.target = target;
	}

	public ListWrapper<V> build()
	{
		ListWrapper<V> listWrapper = new ListWrapper<V>(this.target);
		super.build(listWrapper);
		return listWrapper;
	}

}
