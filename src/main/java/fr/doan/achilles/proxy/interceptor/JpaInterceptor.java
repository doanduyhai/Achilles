package fr.doan.achilles.proxy.interceptor;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.wrapper.builder.ListWrapperBuilder;
import fr.doan.achilles.wrapper.builder.MapWrapperBuilder;
import fr.doan.achilles.wrapper.builder.MultiKeyWideMapWrapperBuilder;
import fr.doan.achilles.wrapper.builder.SetWrapperBuilder;
import fr.doan.achilles.wrapper.builder.WideMapWrapperBuilder;

public class JpaInterceptor<ID> implements MethodInterceptor, AchillesInterceptor
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
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		if (this.idGetter == method)
		{
			return this.key;
		}
		else if (this.idSetter == method)
		{
			throw new IllegalAccessException("Cannot change id value for existing entity ");
		}

		Object result = null;
		if (this.getterMetas.containsKey(method))
		{
			result = interceptGetter(method, args, proxy);
		}
		else if (this.setterMetas.containsKey(method))
		{
			result = interceptSetter(method, args, proxy);
		}
		else
		{
			result = proxy.invoke(target, args);
		}
		return result;
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	private Object interceptGetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		Object result;
		PropertyMeta propertyMeta = this.getterMetas.get(method);
		if (propertyMeta.isLazy() && !this.lazyLoaded.contains(method))
		{
			this.loader.loadPropertyIntoObject(target, key, dao, propertyMeta);
			this.lazyLoaded.add(method);
		}

		switch (propertyMeta.propertyType())
		{
			case LIST:
			case LAZY_LIST:
				List<?> list = (List<?>) proxy.invoke(target, args);
				result = ListWrapperBuilder.builder(list).dirtyMap(dirtyMap)
						.setter(propertyMeta.getSetter()).propertyMeta(propertyMeta).build();
				break;
			case SET:
			case LAZY_SET:
				Set<?> set = (Set<?>) proxy.invoke(target, args);
				result = SetWrapperBuilder.builder(set).dirtyMap(dirtyMap)
						.setter(propertyMeta.getSetter()).propertyMeta(propertyMeta).build();
				break;
			case MAP:
			case LAZY_MAP:
				Map<?, ?> map = (Map<?, ?>) proxy.invoke(target, args);
				result = MapWrapperBuilder.builder(map).dirtyMap(dirtyMap)
						.setter(propertyMeta.getSetter()).propertyMeta(propertyMeta).build();
				break;
			case WIDE_MAP:
				WideMapMeta wideMapMeta = (WideMapMeta) propertyMeta;
				result = buildWideMapWrapper(wideMapMeta);
				break;
			default:
				result = proxy.invoke(target, args);
				break;
		}
		return result;
	}

	private <K extends Comparable<K>, V> Object buildWideMapWrapper(WideMapMeta<K, V> propertyMeta)
	{
		Object result;

		if (propertyMeta.isSingleKey())
		{

			result = WideMapWrapperBuilder.builder(key, dao, propertyMeta).build();
		}
		else
		{
			MultiKeyWideMapMeta<K, V> multiKeyMeta = (MultiKeyWideMapMeta<K, V>) propertyMeta;

			result = MultiKeyWideMapWrapperBuilder.builder(key, dao, multiKeyMeta)
					.componentGetters(multiKeyMeta.getComponentGetters())
					.componentSerializers(multiKeyMeta.getComponentSerializers()).build();
		}

		return result;
	}

	private Object interceptSetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		PropertyMeta<?> propertyMeta = this.setterMetas.get(method);

		if (propertyMeta.propertyType() == PropertyType.WIDE_MAP)
		{
			throw new UnsupportedOperationException(
					"Cannot set value directly to a WideMap structure. Please call the getter first to get handle on the wrapper");
		}

		Object result;
		this.dirtyMap.put(method, propertyMeta);
		result = proxy.invoke(target, args);
		return result;
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
