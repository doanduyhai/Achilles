package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.CQLLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.validation.Validator;

/**
 * CQLEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityLoader implements AchillesEntityLoader<CQLPersistenceContext>
{
	private CQLLoaderImpl loaderImpl;
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();

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
		try
		{

			if (entityMeta.isWideRow() || !context.isLoadEagerFields())
			{
				entity = entityClass.newInstance();
			}
			else
			{
				entity = loaderImpl.eagerLoadEntity(context, entityClass);
			}
			invoker.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);

		}
		catch (Exception e)
		{
			throw new AchillesException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + primaryKey + "'. Cause : "
					+ e.getMessage(), e);
		}
		return entity;
	}

	@Override
	public <V> void loadPropertyIntoObject(Object realObject, Object key,
			CQLPersistenceContext context, PropertyMeta<?, V> propertyMeta)
	{
		CQLPersistenceContext cqlContext = (CQLPersistenceContext) context;
		PropertyType type = propertyMeta.type();
		try
		{
			if (!type.isProxyType())
			{
				if (type.isJoinColumn())
				{
					loaderImpl.loadJoinPropertyIntoEntity(this, cqlContext, propertyMeta,
							realObject);
				}
				else
				{
					loaderImpl.loadPropertyIntoEntity(cqlContext, propertyMeta, realObject);
				}
			}
		}
		catch (Exception e)
		{
			throw new AchillesException("Error when loading property '"
					+ propertyMeta.getPropertyName() + "' into entity '" + realObject
					+ "'. Cause : " + e.getMessage(), e);
		}
	}
}
