package info.archinnov.achilles.holder;

import java.io.Serializable;

/**
 * KeyValue
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValue<K, V> implements Serializable
{
	public static final long serialVersionUID = 1L;

	private final K key;
	private final V value;
	private final int ttl;

	public KeyValue(K key, V value, int ttl) {
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	public KeyValue(K key, V value) {
		this.key = key;
		this.value = value;
		this.ttl = 0;
	}

	public K getKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}

	public int getTtl()
	{
		return ttl;
	}
}
