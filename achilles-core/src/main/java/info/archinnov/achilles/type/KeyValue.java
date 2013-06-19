package info.archinnov.achilles.type;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.annotate.JsonTypeInfo.Id;

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

	@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY)
	private K key;

	@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY)
	private V value;
	private int ttl;
	private long timestamp;

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
	public KeyValue(K key, V value, int ttl, long timestamp) {
		this.key = key;
		this.value = value;
		this.ttl = ttl;
		this.timestamp = timestamp;
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

	public long getTimestamp()
	{
		return timestamp;
	}

	@Override
	public String toString()
	{
		return "KeyValue [key=" + key + ", value=" + value + ", ttl=" + ttl + ", timestamp="
				+ timestamp + "]";
	}

}
