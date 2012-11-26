package fr.doan.achilles.proxy.map;

import java.lang.reflect.Method;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;

public class MapEntryProxy<K, V> implements Map.Entry<K, V>
{

	private final Map.Entry<K, V> target;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<V> propertyMeta;

	public MapEntryProxy(Map.Entry<K, V> target) {
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
		return this.target.getValue();
	}

	@Override
	public V setValue(V value)
	{
		V result = this.target.setValue(value);
		this.markDirty();
		return result;
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
