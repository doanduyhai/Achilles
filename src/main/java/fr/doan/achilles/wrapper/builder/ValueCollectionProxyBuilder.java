package fr.doan.achilles.wrapper.builder;

import java.util.Collection;

import fr.doan.achilles.wrapper.ValueCollectionProxy;

public class ValueCollectionProxyBuilder<V> extends AbstractProxyBuilder<ValueCollectionProxyBuilder<V>, V>
{
	private Collection<V> target;

	public ValueCollectionProxyBuilder(Collection<V> target) {
		this.target = target;
	}

	public static <V> ValueCollectionProxyBuilder<V> builder(Collection<V> target)
	{
		return new ValueCollectionProxyBuilder<V>(target);
	}

	public ValueCollectionProxy<V> build()
	{
		ValueCollectionProxy<V> valueCollectionProxy = new ValueCollectionProxy<V>(this.target);
		super.build(valueCollectionProxy);
		return valueCollectionProxy;
	}

}
