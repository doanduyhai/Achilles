package fr.doan.achilles.proxy.builder;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.collection.ListProxy;

public class ListProxyBuilder<E>
{
	private List<E> target;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<E> propertyMeta;

	public static <E> ListProxyBuilder<E> builder(List<E> target)
	{
		return new ListProxyBuilder<E>(target);
	}

	public ListProxyBuilder(List<E> target) {
		this.target = target;
	}

	public ListProxy<E> build()
	{
		ListProxy<E> listProxy = new ListProxy<E>(this.target);
		listProxy.setDirtyMap(dirtyMap);
		listProxy.setSetter(setter);
		listProxy.setPropertyMeta(propertyMeta);
		return listProxy;
	}

	public ListProxyBuilder<E> dirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return this;
	}

	public ListProxyBuilder<E> setter(Method setter)
	{
		this.setter = setter;
		return this;
	}

	public ListProxyBuilder<E> propertyMeta(PropertyMeta<E> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return this;
	}
}
