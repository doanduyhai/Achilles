package fr.doan.achilles.proxy;

import net.sf.cglib.proxy.Factory;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.AchillesInterceptor;

public class EntityProxyUtil
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
			key = entityMeta.getIdMeta().getGetter().invoke(entity, (Object[]) null);
		}
		catch (Exception e)
		{
			key = null;
		}
		return key;
	}
}
