package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader implements AchillesEntityLoader
{
	private static final Logger log = LoggerFactory.getLogger(EntityLoader.class);

	private EntityIntrospector introspector = new EntityIntrospector();
	private ThriftJoinLoaderImpl joinLoaderImpl = new ThriftJoinLoaderImpl();
	private ThriftLoaderImpl loaderImpl = new ThriftLoaderImpl();

	@Override
	@SuppressWarnings("unchecked")
	public <T, ID> T load(AchillesPersistenceContext<ID> context)
	{
		log.debug("Loading entity of class {} with primary key {}", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

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

			if (entityMeta.isWideRow())
			{
				log.debug("Entity is a wide row, just set the primary key");

				entity = entityClass.newInstance();
				introspector
						.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
			}
			else
			{
				entity = (T) loaderImpl.load((ThriftPersistenceContext<ID>) context);
			}

		}
		catch (Exception e)
		{
			throw new AchillesException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + primaryKey + "'. Cause : "
					+ e.getMessage(), e);
		}
		return (T) entity;
	}

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key,
			AchillesPersistenceContext<ID> context, PropertyMeta<?, V> propertyMeta)
	{
		log.debug("Loading eager properties into entity of class {} with primary key {}", context
				.getEntityClass().getCanonicalName(), context.getPrimaryKey());

		ThriftPersistenceContext<ID> thriftContext = (ThriftPersistenceContext<ID>) context;
		Object value = null;
		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				value = loaderImpl.loadSimpleProperty(thriftContext, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				value = loaderImpl.loadListProperty(thriftContext, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = loaderImpl.loadSetProperty(thriftContext, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = loaderImpl.loadMapProperty(thriftContext, propertyMeta);
				break;
			case JOIN_SIMPLE:
				value = loaderImpl.loadJoinSimple(thriftContext, propertyMeta, this);
				break;
			case JOIN_LIST:
				value = joinLoaderImpl.loadJoinListProperty(thriftContext, propertyMeta);
				break;
			case JOIN_SET:
				value = joinLoaderImpl.loadJoinSetProperty(thriftContext, propertyMeta);
				break;
			case JOIN_MAP:
				value = joinLoaderImpl.loadJoinMapProperty(thriftContext, propertyMeta);
				break;
			default:
				return;
		}
		introspector.setValueToField(realObject, propertyMeta.getSetter(), value);
	}

	protected <ID, V> Long loadVersionSerialUID(ID key, ThriftGenericEntityDao<ID> dao)
	{
		log.debug("Loading serialVersionUID for entity  with primary key {} from column family {}",
				key, dao.getColumnFamily());
		return loaderImpl.loadVersionSerialUID(key, dao);
	}
}
