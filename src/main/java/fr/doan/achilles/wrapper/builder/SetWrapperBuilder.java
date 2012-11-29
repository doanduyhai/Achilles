package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.SetWrapper;

public class SetWrapperBuilder<E> extends AbstractWrapperBuilder<SetWrapperBuilder<E>, E>
{
	private Set<E> target;

	public static <E> SetWrapperBuilder<E> builder(Set<E> target)
	{
		return new SetWrapperBuilder<E>(target);
	}

	public SetWrapperBuilder(Set<E> target) {
		this.target = target;
	}

	public SetWrapper<E> build()
	{
		SetWrapper<E> setWrapper = new SetWrapper<E>(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
