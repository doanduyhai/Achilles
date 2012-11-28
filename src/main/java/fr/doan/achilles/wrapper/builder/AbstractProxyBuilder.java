package fr.doan.achilles.wrapper.builder;

import java.lang.reflect.Method;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.AbstractProxy;

@SuppressWarnings("unchecked")
public abstract class AbstractProxyBuilder<T extends AbstractProxyBuilder<T, E>, E>
{
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<E> propertyMeta;

	public T dirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return (T) this;
	}

	public T setter(Method setter)
	{
		this.setter = setter;
		return (T) this;
	}

	public T propertyMeta(PropertyMeta<E> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return (T) this;
	}

	public void build(AbstractProxy<E> proxy)
	{
		proxy.setDirtyMap(dirtyMap);
		proxy.setSetter(setter);
		proxy.setPropertyMeta(propertyMeta);
	}
}
