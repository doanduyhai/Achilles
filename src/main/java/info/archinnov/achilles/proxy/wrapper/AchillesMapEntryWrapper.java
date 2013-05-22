package info.archinnov.achilles.proxy.wrapper;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesMapEntryWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesMapEntryWrapper<K, V> extends AchillesAbstractWrapper<K, V> implements
		Map.Entry<K, V>
{
	private static final Logger log = LoggerFactory.getLogger(AchillesMapEntryWrapper.class);

	private final Map.Entry<K, V> target;

	public AchillesMapEntryWrapper(Map.Entry<K, V> target) {
		this.target = target;
	}

	@Override
	public K getKey()
	{
		return this.target.getKey();
	}

	@Override
	public V getValue()
	{
		if (isJoin())
		{
			return proxifier
					.buildProxy(this.target.getValue(), joinContext(this.target.getValue()));
		}
		else
		{
			return this.target.getValue();
		}
	}

	@Override
	public V setValue(V value)
	{
		log.trace("Mark map entry property {} of entity class {} dirty upon element set",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		V result = this.target.setValue(proxifier.unproxy(value));
		this.markDirty();
		return result;
	}

	public boolean equals(Entry<K, V> entry)
	{
		K key = entry.getKey();
		V value = proxifier.unproxy(entry.getValue());

		boolean keyEquals = this.target.getKey().equals(key);

		boolean valueEquals = false;
		if (this.target.getValue() == null && value == null)
		{
			valueEquals = true;
		}
		else if (this.target.getValue() != null && value != null)
		{
			valueEquals = this.target.getValue().equals(value);
		}

		return keyEquals && valueEquals;
	}

	public int hashCode()
	{
		K key = this.target.getKey();
		V value = this.target.getValue();
		int result = 1;
		result = result * 31 + key.hashCode();
		result = result * 31 + (value == null ? 0 : value.hashCode());
		return result;
	}

	public Map.Entry<K, V> getTarget()
	{
		return target;
	}

}
