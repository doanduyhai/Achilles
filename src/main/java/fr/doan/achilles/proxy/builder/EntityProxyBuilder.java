package fr.doan.achilles.proxy.builder;

import net.sf.cglib.proxy.Enhancer;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.interceptor.JpaEntityInterceptorBuilder;
import fr.doan.achilles.validation.Validator;

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
}
