package fr.doan.achilles.manager;

import java.io.Serializable;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.operations.EntityLoader;
import fr.doan.achilles.operations.EntityMerger;
import fr.doan.achilles.operations.EntityPersister;
import fr.doan.achilles.proxy.EntityProxyUtil;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;
import fr.doan.achilles.validation.Validator;

@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
public class ThriftEntityManager implements EntityManager
{

	private final Map<Class<?>, EntityMeta<?>> entityMetaMap;

	private EntityPersister persister = new EntityPersister();
	private EntityLoader loader = new EntityLoader();
	private EntityMerger merger = new EntityMerger();
	private EntityProxyUtil util = new EntityProxyUtil();

	private EntityProxyBuilder interceptorBuilder = new EntityProxyBuilder();

	public ThriftEntityManager(Map<Class<?>, EntityMeta<?>> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
	}

	@Override
	public void persist(Object entity)
	{
		this.validateEntity(entity);
		EntityMeta<?> entityMeta = this.entityMetaMap.get(entity.getClass());

		this.persister.persist(entity, entityMeta);
	}

	@Override
	public <T> T merge(T entity)
	{
		this.validateEntity(entity);
		Class baseClass = util.deriveBaseClass(entity);
		EntityMeta<?> entityMeta = this.entityMetaMap.get(baseClass);
		return this.merger.mergeEntity(entity, entityMeta);
	}

	@Override
	public void remove(Object entity)
	{
		this.validateEntity(entity);
		EntityMeta<?> entityMeta = this.entityMetaMap.get(entity.getClass());
		this.persister.remove(entity, entityMeta);
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey)
	{
		Validator.validateNotNull(entityClass, "entity class");
		Validator.validateNotNull(primaryKey, "entity primaryKey");
		Validator.validateSerializable(primaryKey.getClass(), "entity primaryKey");

		EntityMeta<?> entityMeta = this.entityMetaMap.get(entityClass);

		T entity = (T) this.loader.load(entityClass, (Serializable) primaryKey, (EntityMeta) entityMeta);
		return (T) this.interceptorBuilder.build(entity, entityMeta);
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey)
	{
		return this.find(entityClass, primaryKey);
	}

	@Override
	public void flush()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setFlushMode(FlushModeType flushMode)
	{
		throw new UnsupportedOperationException("This operation is not supported for this Entity Manager");

	}

	@Override
	public FlushModeType getFlushMode()
	{
		return FlushModeType.AUTO;
	}

	@Override
	public void lock(Object entity, LockModeType lockMode)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void refresh(Object entity)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void clear()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean contains(Object entity)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Query createQuery(String qlString)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createNamedQuery(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createNativeQuery(String sqlString)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createNativeQuery(String sqlString, Class resultClass)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void joinTransaction()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Object getDelegate()
	{
		return this;
	}

	@Override
	public void close()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isOpen()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public EntityTransaction getTransaction()
	{
		// TODO Auto-generated method stub
		return null;
	}

	private void validateEntity(Object entity)
	{
		Validator.validateNotNull(entity, "entity");

		Class baseClass = util.deriveBaseClass(entity);
		EntityMeta<?> entityMeta = this.entityMetaMap.get(baseClass);

		Validator.validateNotNull(entityMeta, "The entity " + entity.getClass().getCanonicalName() + " is not managed");

		Object key = null;
		try
		{
			key = entityMeta.getIdMeta().getGetter().invoke(entity, (Object[]) null);
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException("Cannot get identifier for entity " + entity.getClass().getCanonicalName());
		}

		Validator.validateNotNull(key, "Cannot get identifier for entity " + entity.getClass().getCanonicalName());
	}
}
