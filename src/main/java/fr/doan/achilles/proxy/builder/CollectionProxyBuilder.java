package fr.doan.achilles.proxy.builder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.collection.CollectionProxy;

public class CollectionProxyBuilder<E>
{
	private Collection<E> target;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Method setter;
	private PropertyMeta<E> propertyMeta;

	public static <E> CollectionProxyBuilder<E> builder(Collection<E> target)
	{
		return new CollectionProxyBuilder<E>(target);
	}

	public CollectionProxyBuilder(Collection<E> target) {
		this.target = target;
	}

	public CollectionProxy<E> build()
	{
		CollectionProxy<E> collectionProxy = new CollectionProxy<E>(this.target);
		collectionProxy.setDirtyMap(dirtyMap);
		collectionProxy.setSetter(setter);
		collectionProxy.setPropertyMeta(propertyMeta);
		return collectionProxy;
	}

	public CollectionProxyBuilder<E> dirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return this;
	}

	public CollectionProxyBuilder<E> setter(Method setter)
	{
		this.setter = setter;
		return this;
	}

	public CollectionProxyBuilder<E> propertyMeta(PropertyMeta<E> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return this;
	}
}
