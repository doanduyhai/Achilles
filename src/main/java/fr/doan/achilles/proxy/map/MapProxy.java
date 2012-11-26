package fr.doan.achilles.proxy.map;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.builder.CollectionProxyBuilder;
import fr.doan.achilles.proxy.builder.SetProxyBuilder;
import fr.doan.achilles.proxy.collection.CollectionProxy;
import fr.doan.achilles.proxy.collection.SetProxy;

public class MapProxy<K, V> implements Map<K, V>
{

	private final Map<K, V> target;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<V> propertyMeta;

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

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet()
	{
		// return this.target.entrySet();
		return null;
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
		if (keySet != null && keySet.size() > 0)
		{
			SetProxy<K> setProxy = SetProxyBuilder.builder(keySet).dirtyMap(dirtyMap).setter(setter).build();
			setProxy.setPropertyMeta((PropertyMeta) propertyMeta);
			keySet = setProxy;
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
			CollectionProxy<V> collectionProxy = CollectionProxyBuilder.builder(values).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta)
					.build();
			values = collectionProxy;
		}
		return values;
	}

	public Map<K, V> getTarget()
	{
		return target;
	}

	public Map<Method, PropertyMeta<?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public void setDirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public void setPropertyMeta(PropertyMeta<V> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
	}

	protected void markDirty()
	{
		if (!dirtyMap.containsKey(this.setter))
		{
			dirtyMap.put(this.setter, this.propertyMeta);
		}
	}
}
