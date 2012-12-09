package fr.doan.achilles.proxy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import net.sf.cglib.proxy.Factory;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.AchillesInterceptor;

public class EntityWrapperUtil
{
	public boolean isProxy(Object entity)
	{
		return Factory.class.isAssignableFrom(entity.getClass());
	}

	@SuppressWarnings("rawtypes")
	public Class deriveBaseClass(Object entity)
	{
		Class baseClass = entity.getClass();
		if (isProxy(entity))
		{
			Factory proxy = (Factory) entity;
			AchillesInterceptor interceptor = (AchillesInterceptor) proxy.getCallback(0);
			baseClass = interceptor.getTarget().getClass();
		}

		return baseClass;
	}

	public Object determinePrimaryKey(Object entity, EntityMeta<?> entityMeta)
	{
		Object key;
		try
		{
			key = entityMeta.getIdMeta().getGetter().invoke(entity);
		}
		catch (Exception e)
		{
			key = null;
		}
		return key;
	}

	public List<Object> determineMultiKey(Object entity, List<Method> componentGetters)
	{
		List<Object> multiKeyValues = new ArrayList<Object>();

		for (Method getter : componentGetters)
		{
			Object key = null;
			try
			{
				key = getter.invoke(entity);
			}
			catch (Exception e)
			{
				// TODO, log error
			}
			multiKeyValues.add(key);
		}

		return multiKeyValues;
	}
}
