package info.archinnov.achilles.proxy.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptorBuilder;
import info.archinnov.achilles.validation.Validator;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;

/**
 * EntityProxyBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityProxyBuilder
{

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <T> T build(T entity, EntityMeta<?> entityMeta)
	{
		Validator.validateNotNull(entity, "entity for proxy builder should not be null");
		Validator.validateNotNull(entityMeta, "entityMeta for proxy builder should not be");

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(JpaEntityInterceptorBuilder.builder((EntityMeta) entityMeta)
				.target(entity).build());

		return (T) enhancer.create();
	}

	public <T> T getRealObject(T proxy)
	{
		Factory factory = (Factory) proxy;
		JpaEntityInterceptor<T> interceptor = (JpaEntityInterceptor<T>) factory.getCallback(0);
		return (T) interceptor.getTarget();
	}
}
