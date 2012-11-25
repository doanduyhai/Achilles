package fr.doan.achilles.proxy.builder;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Set;

import net.sf.cglib.proxy.Enhancer;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.JpaInterceptorBuilder;
import fr.doan.achilles.validation.Validator;

public class EntityProxyBuilder<ID extends Serializable>
{

	public <T> T build(T entity, EntityMeta<ID> entityMeta)
	{
		return this.build(entity, entityMeta, null);
	}

	@SuppressWarnings("unchecked")
	public <T> T build(T entity, EntityMeta<ID> entityMeta, Set<Method> lazyLoaded)
	{
		Validator.validateNotNull(entity, "entity");
		Validator.validateNotNull(entityMeta, "entityMeta");

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(JpaInterceptorBuilder.builder(entityMeta).target(entity).lazyLoaded(lazyLoaded).build());

		return (T) enhancer.create();
	}
}
