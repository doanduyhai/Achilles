package fr.doan.achilles.proxy.builder;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.collection.SetProxy;

public class SetProxyBuilder<E>
{
	private Set<E> target;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<E> propertyMeta;

	public static <E> SetProxyBuilder<E> builder(Set<E> target)
	{
		return new SetProxyBuilder<E>(target);
	}

	public SetProxyBuilder(Set<E> target) {
		this.target = target;
	}

	public SetProxy<E> build()
	{
		SetProxy<E> setProxy = new SetProxy<E>(this.target);
		setProxy.setDirtyMap(dirtyMap);
		setProxy.setSetter(setter);
		setProxy.setPropertyMeta(propertyMeta);
		return setProxy;
	}

	public SetProxyBuilder<E> dirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return this;
	}

	public SetProxyBuilder<E> setter(Method setter)
	{
		this.setter = setter;
		return this;
	}

	public SetProxyBuilder<E> propertyMeta(PropertyMeta<E> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return this;
	}
}
