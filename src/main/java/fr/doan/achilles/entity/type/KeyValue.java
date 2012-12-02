package fr.doan.achilles.entity.type;


/**
 * KeyValue
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValue<K, V>
{
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
