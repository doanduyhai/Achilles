/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.type;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.annotate.JsonTypeInfo.Id;

public class KeyValue<K, V> {
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
	public KeyValue() {
	}

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
	public K getKey() {
		return key;
	}

	/**
	 * Get the value, can be null
	 * 
	 * @return value
	 */
	public V getValue() {
		return value;
	}

	/**
	 * Get the time to live, can be null
	 * 
	 * If null, the value never expires
	 * 
	 * @return ttl
	 */
	public int getTtl() {
		return ttl;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", value=" + value + ", ttl=" + ttl
				+ ", timestamp=" + timestamp + "]";
	}

}
