package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.entity.context.PersistenceContext;
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
	private PersistenceContext<ID> context;
	private EntityLoader loader = new EntityLoader();

	public static <ID, T> JpaEntityInterceptorBuilder<ID, T> builder(
			PersistenceContext<ID> context, T entity)
	{
		return new JpaEntityInterceptorBuilder<ID, T>(context, entity);
	}

	public JpaEntityInterceptorBuilder(PersistenceContext<ID> context, T entity) {
		Validator.validateNotNull(context, "PersistenceContext for interceptor should not be null");
		Validator.validateNotNull(entity, "Target entity for interceptor should not be null");
		this.context = context;
		this.target = entity;
	}

	public JpaEntityInterceptorBuilder<ID, T> lazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
		return this;
	}

	public JpaEntityInterceptor<ID, T> build()
	{
		JpaEntityInterceptor<ID, T> interceptor = new JpaEntityInterceptor<ID, T>();

		EntityMeta<ID> entityMeta = context.getEntityMeta();

		Validator.validateNotNull(this.target, "Target object for interceptor of '"
				+ context.getEntityClass().getCanonicalName() + "' should not be null");
		Validator.validateNotNull(entityMeta.getGetterMetas(),
				"Getters metadata for interceptor of '"
						+ context.getEntityClass().getCanonicalName() + "' should not be null");
		Validator.validateNotNull(entityMeta.getSetterMetas(),
				"Setters metadata for interceptor of '"
						+ context.getEntityClass().getCanonicalName() + "'should not be null");
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			Validator.validateNotNull(context.getColumnFamilyDao(), "Column Family Dao for '"
					+ context.getEntityClass().getCanonicalName() + "' should not be null");
		}
		else
		{
			Validator.validateNotNull(context.getEntityDao(), "Entity dao for '"
					+ context.getEntityClass().getCanonicalName() + "' should not be null");
		}
		Validator.validateNotNull(entityMeta.getIdMeta(), "Id metadata for '"
				+ context.getEntityClass().getCanonicalName() + "' should not be null");

		interceptor.setTarget(target);
		interceptor.setContext(context);
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
		interceptor.setKey(context.getPrimaryKey());
		interceptor.setLoader(loader);

		return interceptor;
	}
}
