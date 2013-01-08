package fr.doan.achilles.wrapper.builder;

import java.util.Set;

import fr.doan.achilles.wrapper.AbstractWrapper;
import fr.doan.achilles.wrapper.KeySetWrapper;

/**
 * KeySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeySetWrapperBuilder<K> extends
		AbstractWrapperBuilder<KeySetWrapperBuilder<K>, K, Void>
{
	private Set<K> target;

	public KeySetWrapperBuilder(Set<K> target) {
		this.target = target;
	}

	public static <K> KeySetWrapperBuilder<K> builder(Set<K> target)
	{
		return new KeySetWrapperBuilder<K>(target);
	}

	@SuppressWarnings("unchecked")
	public KeySetWrapper<K> build()
	{
		KeySetWrapper<K> keySetWrapper = new KeySetWrapper<K>(this.target);
		super.build((AbstractWrapper<K, Void>) keySetWrapper);
		return keySetWrapper;
	}

}
