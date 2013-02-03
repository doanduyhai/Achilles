package info.archinnov.achilles.wrapper.builder;

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

	public static <V> SetWrapperBuilder<V> builder(Set<V> target)
	{
		return new SetWrapperBuilder<V>(target);
	}

	public SetWrapperBuilder(Set<V> target) {
		this.target = target;
	}

	public SetWrapper<V> build()
	{
		SetWrapper<V> setWrapper = new SetWrapper<V>(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
