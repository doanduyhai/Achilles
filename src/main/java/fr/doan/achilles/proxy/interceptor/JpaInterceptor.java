package fr.doan.achilles.proxy.interceptor;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.operations.EntityLoader;

public class JpaInterceptor<ID extends Serializable> implements MethodInterceptor, AchillesInterceptor
{

	private Object target;
	private GenericDao<ID> dao;
	private ID key;
	private Method idGetter;
	private Method idSetter;
	private Map<Method, PropertyMeta<?>> getterMetas;
	private Map<Method, PropertyMeta<?>> setterMetas;
	private Map<Method, PropertyMeta<?>> dirtyMap;
	private Set<Method> lazyLoaded;

	private EntityLoader loader;

	@Override
	public Object getTarget()
	{
		return this.target;
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable
	{
		if (this.idGetter == method)
		{
			return this.key;
		}
		else if (this.idSetter == method)
		{
			throw new IllegalAccessException("Cannot change id value for existing entity ");
		}

		if (this.getterMetas.containsKey(method))
		{
			PropertyMeta<?> propertyMeta = this.getterMetas.get(method);
			if (propertyMeta.isLazy() && !this.lazyLoaded.contains(method))
			{
				this.loader.loadPropertyIntoObject(target, key, dao, propertyMeta);
				this.lazyLoaded.add(method);
			}
		}
		else if (this.setterMetas.containsKey(method))
		{
			this.dirtyMap.put(method, this.setterMetas.get(method));
		}
		return proxy.invoke(target, args);
	}

	public Map<Method, PropertyMeta<?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public Set<Method> getLazyLoaded()
	{
		return lazyLoaded;
	}

	@Override
	public ID getKey()
	{
		return key;
	}

	public void setTarget(Object target)
	{
		this.target = target;
	}

	void setDao(GenericDao<ID> dao)
	{
		this.dao = dao;
	}

	void setKey(ID key)
	{
		this.key = key;
	}

	void setIdGetter(Method idGetter)
	{
		this.idGetter = idGetter;
	}

	void setIdSetter(Method idSetter)
	{
		this.idSetter = idSetter;
	}

	void setGetterMetas(Map<Method, PropertyMeta<?>> getterMetas)
	{
		this.getterMetas = getterMetas;
	}

	void setSetterMetas(Map<Method, PropertyMeta<?>> setterMetas)
	{
		this.setterMetas = setterMetas;
	}

	void setDirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	void setLazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
	}

	void setLoader(EntityLoader loader)
	{
		this.loader = loader;
	}

}
