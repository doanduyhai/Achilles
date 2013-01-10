package fr.doan.achilles.holder;

import java.io.Serializable;

/**
 * KeyValueHolder
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueHolder<K, V> implements Serializable
{

	public static final long serialVersionUID = 1L;

	private final K key;
	private final V value;

	public KeyValueHolder(K key, V value) {
		super();
		this.key = key;
		this.value = value;
	}

	public K getKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}
}
