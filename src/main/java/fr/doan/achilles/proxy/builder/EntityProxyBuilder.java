package fr.doan.achilles.proxy.builder;

import java.io.Serializable;

import net.sf.cglib.proxy.Enhancer;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.JpaInterceptorBuilder;
import fr.doan.achilles.validation.Validator;

public class EntityProxyBuilder<ID extends Serializable>
{

	@SuppressWarnings("unchecked")
	public <T> T build(T entity, EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entity, "entity");
		Validator.validateNotNull(entityMeta, "entityMeta");

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(JpaInterceptorBuilder.builder(entityMeta).target(entity).build());

		return (T) enhancer.create();
	}
}
