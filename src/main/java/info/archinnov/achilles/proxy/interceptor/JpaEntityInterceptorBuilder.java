package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.validation.Validator;

import java.io.Serializable;
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
public class JpaEntityInterceptorBuilder<ID extends Serializable>
{

	private Object target;
	private Set<Method> lazyLoaded = new HashSet<Method>();
	private EntityMeta<ID> entityMeta;
	private EntityHelper helper = new EntityHelper();

	public static <ID extends Serializable> JpaEntityInterceptorBuilder<ID> builder(
			EntityMeta<ID> entityMeta)
	{
		return new JpaEntityInterceptorBuilder<ID>(entityMeta);
	}

	public JpaEntityInterceptorBuilder(EntityMeta<ID> entityMeta) {
		Validator.validateNotNull(entityMeta, "EntityMeta for interceptor should not be null");
		this.entityMeta = entityMeta;
	}

	public JpaEntityInterceptorBuilder<ID> target(Object target)
	{
		Validator.validateNotNull(target, "Target object for interceptor should not be null");
		this.target = target;
		return this;
	}

	public JpaEntityInterceptorBuilder<ID> lazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
		return this;
	}

	@SuppressWarnings("unchecked")
	public JpaEntityInterceptor<ID> build()
	{
		JpaEntityInterceptor<ID> interceptor = new JpaEntityInterceptor<ID>();

		Validator.validateNotNull(this.target, "Target object for interceptor should not be null");
		Validator.validateNotNull(entityMeta.getGetterMetas(),
				"Getters metadata for interceptor should not be null");
		Validator.validateNotNull(entityMeta.getSetterMetas(),
				"Setters metadata for interceptor should not be null");
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			interceptor.setColumnFamily(true);
			Validator.validateNotNull(entityMeta.getColumnFamilyDao(), "Dao for entity meta");
			interceptor.setColumnFamilyDao(entityMeta.getColumnFamilyDao());
		}
		else
		{
			interceptor.setColumnFamily(false);
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

		interceptor.setLoader(new EntityLoader());
		return interceptor;
	}
}
