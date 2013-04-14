package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader
{

	private EntityIntrospector introspector = new EntityIntrospector();
	private ThriftJoinLoaderImpl joinLoaderImpl = new ThriftJoinLoaderImpl();
	private ThriftLoaderImpl loaderImpl = new ThriftLoaderImpl();

	@SuppressWarnings("unchecked")
	public <T, ID> T load(PersistenceContext<ID> context)
	{
		Class<T> entityClass = (Class<T>) context.getEntityClass();
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '" + entityClass.getCanonicalName()
				+ "' key should not be null");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		T entity = null;
		try
		{

			if (entityMeta.isColumnFamilyDirectMapping())
			{
				entity = entityClass.newInstance();
				introspector
						.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
			}
			else
			{
				entity = loaderImpl.load(context);
			}

		}
		catch (Exception e)
		{
			throw new AchillesException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + primaryKey + "'. Cause : "
					+ e.getMessage(), e);
		}
		return entity;
	}

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key,
			PersistenceContext<ID> context, PropertyMeta<?, V> propertyMeta)
	{
		Object value = null;
		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				value = loaderImpl.loadSimpleProperty(context, propertyMeta);
				break;
			case COUNTER:
				value = loaderImpl.loadSimpleCounterProperty(context, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				value = loaderImpl.loadListProperty(context, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = loaderImpl.loadSetProperty(context, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = loaderImpl.loadMapProperty(context, propertyMeta);
				break;
			case JOIN_SIMPLE:
				value = loaderImpl.loadJoinSimple(context, propertyMeta, this);
				break;
			case JOIN_LIST:
				value = joinLoaderImpl.loadJoinListProperty(context, propertyMeta);
				break;
			case JOIN_SET:
				value = joinLoaderImpl.loadJoinSetProperty(context, propertyMeta);
				break;
			case JOIN_MAP:
				value = joinLoaderImpl.loadJoinMapProperty(context, propertyMeta);
				break;
			default:
				return;
		}
		introspector.setValueToField(realObject, propertyMeta.getSetter(), value);
	}

	protected <ID, V> Long loadVersionSerialUID(ID key, GenericEntityDao<ID> dao)
	{
		return loaderImpl.loadVersionSerialUID(key, dao);
	}
}
