package info.archinnov.achilles.entity.type;

import java.util.Iterator;

/**
 * KeyValueIterator
 * 
 * Iterator for key/value pair
 * 
 * @author DuyHai DOAN
 * 
 */
public interface KeyValueIterator<K, V> extends Iterator<KeyValue<K, V>>
{

	/**
	 * Return the next key
	 * 
	 * @return next key
	 */
	public K nextKey();

	/**
	 * Return the next value
	 * 
	 * @return next value
	 */
	public V nextValue();

	/**
	 * Return the next ttl value, if any
	 * 
	 * @return next ttl value
	 */
	public Integer nextTtl();
}
