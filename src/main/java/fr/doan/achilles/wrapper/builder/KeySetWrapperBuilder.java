package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.KeySetWrapper;

public class KeySetWrapperBuilder<K> extends AbstractWrapperBuilder<KeySetWrapperBuilder<K>, K>
{
	private Set<K> target;

	public KeySetWrapperBuilder(Set<K> target) {
		this.target = target;
	}

	public static <K> KeySetWrapperBuilder<K> builder(Set<K> target)
	{
		return new KeySetWrapperBuilder<K>(target);
	}

	public KeySetWrapper<K> build()
	{
		KeySetWrapper<K> keySetWrapper = new KeySetWrapper<K>(this.target);
		super.build(keySetWrapper);
		return keySetWrapper;
	}

}
