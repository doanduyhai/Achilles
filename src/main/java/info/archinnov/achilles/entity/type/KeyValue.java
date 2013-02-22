package info.archinnov.achilles.entity.type;

import java.io.Serializable;

/**
 * KeyValue
 * 
 * Holder structure for key/value pair
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValue<K, V> implements Serializable
{
	public static final long serialVersionUID = 1L;

	private K key;
	private V value;
	private int ttl;

	/**
	 * Default constructor
	 */
	public KeyValue() {}

	/**
	 * Create a KeyValue holder with ttl
	 * 
	 * @param key
	 *            Key
	 * @param value
	 *            Value
	 * @param ttl
	 *            Time to live
	 */
	public KeyValue(K key, V value, int ttl) {
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	/**
	 * Create a KeyValue holder
	 * 
	 * @param key
	 *            Key
	 * @param value
	 *            Value
	 */
	public KeyValue(K key, V value) {
		this.key = key;
		this.value = value;
		this.ttl = 0;
	}

	/**
	 * Get the key
	 * 
	 * @return key
	 */
	public K getKey()
	{
		return key;
	}

	/**
	 * Get the value, can be null
	 * 
	 * @return value
	 */
	public V getValue()
	{
		return value;
	}

	/**
	 * Get the time to live, can be null
	 * 
	 * If null, the value never expires
	 * 
	 * @return ttl
	 */
	public int getTtl()
	{
		return ttl;
	}
}
