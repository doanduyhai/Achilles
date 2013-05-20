package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesEntityProxifier
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityProxifier.class);

	public <ID> Class<?> deriveBaseClass(Object entity)
	{
		log.debug("Deriving base class for entity {} ", entity);

		Class<?> baseClass = entity.getClass();
		if (isProxy(entity))
		{
			AchillesJpaEntityInterceptor<?> interceptor = getInterceptor(entity);
			baseClass = interceptor.getTarget().getClass();
		}

		return baseClass;
	}

	public <T> T buildProxy(T entity, AchillesPersistenceContext context)
	{

		if (entity == null)
		{
			return null;
		}

		log.debug("Build Cglib proxy for entity {} ", entity);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(buildInterceptor(context, entity));

		return (T) enhancer.create();
	}

	public <T> T getRealObject(T proxy)
	{
		log.debug("Get real entity from proxy {} ", proxy);

		Factory factory = (Factory) proxy;
		AchillesJpaEntityInterceptor<?> interceptor = (AchillesJpaEntityInterceptor<?>) factory
				.getCallback(0);
		return (T) interceptor.getTarget();
	}

	public boolean isProxy(Object entity)
	{
		return Factory.class.isAssignableFrom(entity.getClass());
	}

	public <T> AchillesJpaEntityInterceptor<T> getInterceptor(T proxy)
	{
		log.debug("Get JPA interceptor from proxy {} ", proxy);

		Factory factory = (Factory) proxy;
		AchillesJpaEntityInterceptor<T> interceptor = (AchillesJpaEntityInterceptor<T>) factory
				.getCallback(0);
		return interceptor;
	}

	public <T> void ensureProxy(T proxy)
	{
		if (!isProxy(proxy))
		{
			throw new IllegalStateException("The entity '" + proxy + "' is not in 'managed' state.");
		}
	}

	public <T> T unproxy(T proxy)
	{
		log.debug("Unproxying object {} ", proxy);

		if (proxy != null)
		{

			if (isProxy(proxy))
			{
				return getRealObject(proxy);
			}
			else
			{
				return proxy;
			}
		}
		else
		{
			return null;
		}
	}

	public <K, V> Entry<K, V> unproxy(Entry<K, V> entry)
	{
		V value = entry.getValue();
		if (isProxy(value))
		{
			value = getRealObject(value);
			entry.setValue(value);
		}
		return entry;
	}

	public <T> Collection<T> unproxy(Collection<T> proxies)
	{
		Collection<T> result = new ArrayList<T>();
		for (T proxy : proxies)
		{
			result.add(unproxy(proxy));
		}
		return result;
	}

	public <T> List<T> unproxy(List<T> proxies)
	{
		List<T> result = new ArrayList<T>();
		for (T proxy : proxies)
		{
			result.add(this.unproxy(proxy));
		}

		return result;
	}

	public <T> Set<T> unproxy(Set<T> proxies)
	{
		Set<T> result = new HashSet<T>();
		for (T proxy : proxies)
		{
			result.add(this.unproxy(proxy));
		}

		return result;
	}

	public abstract <T> AchillesJpaEntityInterceptor<T> buildInterceptor(
			AchillesPersistenceContext context, T entity);
}
