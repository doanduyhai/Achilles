package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.SetWrapper;

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
