package fr.doan.achilles.wrapper.builder;

import java.lang.reflect.Method;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.AbstractWrapper;

@SuppressWarnings("unchecked")
public abstract class AbstractWrapperBuilder<T extends AbstractWrapperBuilder<T, K, V>, K, V>
{
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Method setter;
	private PropertyMeta<K, V> propertyMeta;

	public T dirtyMap(Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return (T) this;
	}

	public T setter(Method setter)
	{
		this.setter = setter;
		return (T) this;
	}

	public T propertyMeta(PropertyMeta<K, V> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return (T) this;
	}

	public void build(AbstractWrapper<K, V> wrapper)
	{
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
	}
}
