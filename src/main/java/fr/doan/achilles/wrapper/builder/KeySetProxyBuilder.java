package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.KeySetProxy;

public class KeySetProxyBuilder<K> extends AbstractProxyBuilder<KeySetProxyBuilder<K>, K>
{
	private Set<K> target;

	public KeySetProxyBuilder(Set<K> target) {
		this.target = target;
	}

	public static <K> KeySetProxyBuilder<K> builder(Set<K> target)
	{
		return new KeySetProxyBuilder<K>(target);
	}

	public KeySetProxy<K> build()
	{
		KeySetProxy<K> keySetProxy = new KeySetProxy<K>(this.target);
		super.build(keySetProxy);
		return keySetProxy;
	}

}
