package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.wrapper.SetWrapper;

import java.util.Set;

/**
 * SetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapperBuilder<V> extends AbstractWrapperBuilder<SetWrapperBuilder<V>, Void, V>
{
	private Set<V> target;

	public static <ID, V> SetWrapperBuilder<V> builder(AchillesPersistenceContext context,
			Set<V> target)
	{
		return new SetWrapperBuilder<V>(context, target);
	}

	public SetWrapperBuilder(AchillesPersistenceContext context, Set<V> target) {
		super.context = context;
		this.target = target;
	}

	public SetWrapper<V> build()
	{
		SetWrapper<V> setWrapper = new SetWrapper<V>(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
