package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.wrapper.builder.EntrySetWrapperBuilder.builder;
import static info.archinnov.achilles.wrapper.builder.KeySetWrapperBuilder.builder;
import static info.archinnov.achilles.wrapper.builder.ValueCollectionWrapperBuilder.builder;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * MapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapWrapper<K, V> extends AbstractWrapper<K, V> implements Map<K, V>
{

	private final Map<K, V> target;

	public MapWrapper(Map<K, V> target) {
		this.target = target;
	}

	@Override
	public void clear()
	{
		if (this.target.size() > 0)
		{
			this.markDirty();
		}
		this.target.clear();

	}

	@Override
	public boolean containsKey(Object key)
	{
		return this.target.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value)
	{
		return this.target.containsValue(value);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet()
	{
		Set<Entry<K, V>> targetEntrySet = this.target.entrySet();
		if (targetEntrySet.size() > 0)
		{
			EntrySetWrapper<K, V> wrapperSet = builder(targetEntrySet).dirtyMap(dirtyMap)
					.setter(setter).propertyMeta((PropertyMeta) propertyMeta).build();
			targetEntrySet = wrapperSet;
		}
		return targetEntrySet;
	}

	@Override
	public V get(Object key)
	{
		return this.target.get(key);
	}

	@Override
	public boolean isEmpty()
	{
		return this.target.isEmpty();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public Set<K> keySet()
	{
		Set<K> keySet = this.target.keySet();
		if (keySet.size() > 0)
		{
			KeySetWrapper<K> keySetWrapper = builder(keySet).dirtyMap(dirtyMap).setter(setter)
					.propertyMeta((PropertyMeta) propertyMeta).build();
			keySet = keySetWrapper;
		}
		return keySet;
	}

	@Override
	public V put(K key, V value)
	{
		V result = this.target.put(key, value);
		this.markDirty();
		return result;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m)
	{
		this.target.putAll(m);
		this.markDirty();
	}

	@Override
	public V remove(Object key)
	{
		if (this.target.containsKey(key))
		{
			this.markDirty();
		}
		return this.target.remove(key);
	}

	@Override
	public int size()
	{
		return this.target.size();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public Collection<V> values()
	{
		Collection<V> values = this.target.values();

		if (values.size() > 0)
		{
			ValueCollectionWrapper<V> collectionWrapper = builder(values).dirtyMap(dirtyMap)
					.setter(setter).propertyMeta((PropertyMeta) propertyMeta).build();
			values = collectionWrapper;
		}
		return values;
	}

	public Map<K, V> getTarget()
	{
		return target;
	}
}
