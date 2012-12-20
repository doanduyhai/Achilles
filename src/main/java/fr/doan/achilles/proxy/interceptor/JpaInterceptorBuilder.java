package fr.doan.achilles.proxy.interceptor;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.validation.Validator;

public class JpaInterceptorBuilder<ID extends Serializable>
{

	private Object target;
	private Set<Method> lazyLoaded = new HashSet<Method>();
	private EntityMeta<ID> entityMeta;

	public static <ID extends Serializable> JpaInterceptorBuilder<ID> builder(
			EntityMeta<ID> entityMeta)
	{
		return new JpaInterceptorBuilder<ID>(entityMeta);
	}

	public JpaInterceptorBuilder(EntityMeta<ID> entityMeta) {
		Validator.validateNotNull(entityMeta, "entityMeta");
		this.entityMeta = entityMeta;
	}

	public JpaInterceptorBuilder<ID> target(Object target)
	{
		Validator.validateNotNull(target, "Target object");
		this.target = target;
		return this;
	}

	public JpaInterceptorBuilder<ID> lazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
		return this;
	}

	@SuppressWarnings("unchecked")
	public JpaEntityInterceptor<ID> build()
	{
		JpaEntityInterceptor<ID> interceptor = new JpaEntityInterceptor<ID>();

		Validator.validateNotNull(this.target, "Target object");
		Validator.validateNotNull(entityMeta.getGetterMetas(), "Getters metadata");
		Validator.validateNotNull(entityMeta.getSetterMetas(), "Setters metadata");
		Validator.validateNotNull(entityMeta.getEntityDao(), "Dao for entity meta");
		Validator.validateNotNull(entityMeta.getIdMeta(), "Id metadata");

		interceptor.setTarget(target);
		interceptor.setGetterMetas(entityMeta.getGetterMetas());
		interceptor.setSetterMetas(entityMeta.getSetterMetas());
		interceptor.setDao(entityMeta.getEntityDao());
		interceptor.setIdGetter(entityMeta.getIdMeta().getGetter());
		interceptor.setIdSetter(entityMeta.getIdMeta().getSetter());

		if (this.lazyLoaded == null)
		{
			this.lazyLoaded = new HashSet<Method>();
		}
		interceptor.setLazyLoaded(this.lazyLoaded);
		interceptor.setDirtyMap(new HashMap<Method, PropertyMeta<?, ?>>());

		try
		{
			interceptor.setKey((ID) entityMeta.getIdMeta().getGetter().invoke(target));
		}
		catch (Exception e)
		{}

		interceptor.setLoader(new EntityLoader());
		return interceptor;
	}
}
