package fr.doan.achilles.wrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.builder.KeySetProxyBuilder;
import fr.doan.achilles.wrapper.builder.MapEntryProxyBuilder;
import fr.doan.achilles.wrapper.builder.SetProxyBuilder;
import fr.doan.achilles.wrapper.builder.ValueCollectionProxyBuilder;

public class MapProxy<K, V> extends AbstractProxy<V> implements Map<K, V>
{

	private final Map<K, V> target;

	public MapProxy(Map<K, V> target) {
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
			SetProxy<Entry<K, V>> wrapperSet = SetProxyBuilder.builder(targetEntrySet).dirtyMap(dirtyMap).setter(setter)
					.propertyMeta((PropertyMeta) propertyMeta).build();
			for (Entry<K, V> entry : targetEntrySet)
			{
				MapEntryProxy<K, V> entryProxy = MapEntryProxyBuilder.builder(entry).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta)
						.build();
				wrapperSet.add(entryProxy);
			}
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
			KeySetProxy<K> keySetProxy = KeySetProxyBuilder.builder(keySet).dirtyMap(dirtyMap).setter(setter)
					.propertyMeta((PropertyMeta) propertyMeta).build();
			keySet = keySetProxy;
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

	@Override
	public Collection<V> values()
	{
		Collection<V> values = this.target.values();

		if (values != null && values.size() > 0)
		{
			ValueCollectionProxy<V> collectionProxy = ValueCollectionProxyBuilder.builder(values).dirtyMap(dirtyMap).setter(setter)
					.propertyMeta(propertyMeta).build();
			values = collectionProxy;
		}
		return values;
	}

	public Map<K, V> getTarget()
	{
		return target;
	}
}
