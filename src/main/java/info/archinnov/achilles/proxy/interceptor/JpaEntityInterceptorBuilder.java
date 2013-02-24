package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * JpaEntityInterceptorBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class JpaEntityInterceptorBuilder<ID, T>
{

	private T target;
	private Set<Method> lazyLoaded = new HashSet<Method>();
	private EntityMeta<ID> entityMeta;
	private EntityHelper helper = new EntityHelper();
	private EntityLoader loader = new EntityLoader();

	public static <ID, T> JpaEntityInterceptorBuilder<ID, T> builder(EntityMeta<ID> entityMeta,
			T entity)
	{
		return new JpaEntityInterceptorBuilder<ID, T>(entityMeta, entity);
	}

	public JpaEntityInterceptorBuilder(EntityMeta<ID> entityMeta, T entity) {
		Validator.validateNotNull(entityMeta, "EntityMeta for interceptor should not be null");
		Validator.validateNotNull(entity, "Target object for interceptor should not be null");
		this.entityMeta = entityMeta;
		this.target = entity;
	}

	public JpaEntityInterceptorBuilder<ID, T> lazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
		return this;
	}

	@SuppressWarnings("unchecked")
	public JpaEntityInterceptor<ID, T> build()
	{
		JpaEntityInterceptor<ID, T> interceptor = new JpaEntityInterceptor<ID, T>();

		Validator.validateNotNull(this.target, "Target object for interceptor should not be null");
		Validator.validateNotNull(entityMeta.getGetterMetas(),
				"Getters metadata for interceptor should not be null");
		Validator.validateNotNull(entityMeta.getSetterMetas(),
				"Setters metadata for interceptor should not be null");
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			interceptor.setDirectColumnFamilyMapping(true);
			Validator.validateNotNull(entityMeta.getColumnFamilyDao(), "Dao for entity meta");
			interceptor.setColumnFamilyDao(entityMeta.getColumnFamilyDao());
		}
		else
		{
			interceptor.setDirectColumnFamilyMapping(false);
			Validator.validateNotNull(entityMeta.getEntityDao(), "Dao for entity meta");
			interceptor.setEntityDao(entityMeta.getEntityDao());

		}
		Validator.validateNotNull(entityMeta.getIdMeta(), "Id metadata");

		interceptor.setTarget(target);
		interceptor.setGetterMetas(entityMeta.getGetterMetas());
		interceptor.setSetterMetas(entityMeta.getSetterMetas());
		interceptor.setIdGetter(entityMeta.getIdMeta().getGetter());
		interceptor.setIdSetter(entityMeta.getIdMeta().getSetter());

		if (this.lazyLoaded == null)
		{
			this.lazyLoaded = new HashSet<Method>();
		}
		interceptor.setLazyLoaded(this.lazyLoaded);
		interceptor.setDirtyMap(new HashMap<Method, PropertyMeta<?, ?>>());
		interceptor.setKey((ID) helper
				.getValueFromField(target, entityMeta.getIdMeta().getGetter()));

		interceptor.setLoader(loader);
		return interceptor;
	}
}
