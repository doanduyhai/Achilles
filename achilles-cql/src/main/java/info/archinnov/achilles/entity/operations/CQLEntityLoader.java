package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.CQLLoaderImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;

/**
 * CQLEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityLoader implements EntityLoader<CQLPersistenceContext>
{
	private CQLLoaderImpl loaderImpl = new CQLLoaderImpl();
	private ReflectionInvoker invoker = new ReflectionInvoker();
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();

	@Override
	public <T> T load(CQLPersistenceContext context, Class<T> entityClass)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '" + entityClass.getCanonicalName()
				+ "' key should not be null");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		T entity = null;

		if (context.isLoadEagerFields())
		{
			entity = loaderImpl.eagerLoadEntity(context, entityClass);
		}
		else
		{
			entity = invoker.instanciate(entityClass);
		}
		invoker.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);

		return entity;
	}

	@Override
	public <V> void loadPropertyIntoObject(CQLPersistenceContext context, Object realObject,
			PropertyMeta<?, V> pm)
	{
		PropertyType type = pm.type();
		if (!type.isProxyType())
		{
			if (type.isJoin())
			{
				loaderImpl.loadJoinPropertyIntoEntity(this, context, pm,
						realObject);
			}
			else
			{
				loaderImpl.loadPropertyIntoEntity(context, pm, realObject);
			}
		}
	}

}
