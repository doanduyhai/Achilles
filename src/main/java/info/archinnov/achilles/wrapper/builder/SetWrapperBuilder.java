package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.wrapper.SetWrapper;

import java.util.Set;

/**
 * SetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapperBuilder<ID, V> extends
		AbstractWrapperBuilder<ID, SetWrapperBuilder<ID, V>, Void, V>
{
	private Set<V> target;

	public static <ID, V> SetWrapperBuilder<ID, V> builder(PersistenceContext<ID> context,
			Set<V> target)
	{
		return new SetWrapperBuilder<ID, V>(context, target);
	}

	public SetWrapperBuilder(PersistenceContext<ID> context, Set<V> target) {
		super.context = context;
		this.target = target;
	}

	public SetWrapper<ID, V> build()
	{
		SetWrapper<ID, V> setWrapper = new SetWrapper<ID, V>(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
