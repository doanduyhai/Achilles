package fr.doan.achilles.proxy.builder;

import net.sf.cglib.proxy.Enhancer;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.JpaInterceptorBuilder;
import fr.doan.achilles.validation.Validator;

public class EntityProxyBuilder
{

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <T> T build(T entity, EntityMeta<?> entityMeta)
	{
		Validator.validateNotNull(entity, "entity");
		Validator.validateNotNull(entityMeta, "entityMeta");

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(JpaInterceptorBuilder.builder((EntityMeta) entityMeta).target(entity)
				.build());

		return (T) enhancer.create();
	}
}
