package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.SetProxy;

public class SetProxyBuilder<E> extends AbstractProxyBuilder<SetProxyBuilder<E>, E>
{
	private Set<E> target;

	public static <E> SetProxyBuilder<E> builder(Set<E> target)
	{
		return new SetProxyBuilder<E>(target);
	}

	public SetProxyBuilder(Set<E> target) {
		this.target = target;
	}

	public SetProxy<E> build()
	{
		SetProxy<E> setProxy = new SetProxy<E>(this.target);
		super.build(setProxy);
		return setProxy;
	}

}
