package fr.doan.achilles.entity.manager;

import java.io.Serializable;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityMerger;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.entity.operations.EntityRefresher;
import fr.doan.achilles.entity.operations.EntityValidator;
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
	private EntityRefresher entityRefresher = new EntityRefresher();
	private EntityHelper helper = new EntityHelper();
	private EntityValidator entityValidator = new EntityValidator();

	private EntityProxyBuilder interceptorBuilder = new EntityProxyBuilder();

	public ThriftEntityManager(Map<Class<?>, EntityMeta<?>> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
	}

	@Override
	public void persist(Object entity)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		if (helper.isProxy(entity))
		{
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}

		EntityMeta<?> entityMeta = this.entityMetaMap.get(entity.getClass());

		this.persister.persist(entity, entityMeta);
	}

	@Override
	public <T> T merge(T entity)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		Class baseClass = helper.deriveBaseClass(entity);
		EntityMeta<?> entityMeta = this.entityMetaMap.get(baseClass);
		return this.merger.mergeEntity(entity, entityMeta);
	}

	@Override
	public void remove(Object entity)
	{
		entityValidator.validateEntity(entity, entityMetaMap);

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

		T entity = (T) this.loader.load(entityClass, (Serializable) primaryKey,
				(EntityMeta) entityMeta);

		if (entity != null)
		{
			entity = (T) this.interceptorBuilder.build(entity, entityMeta);
		}

		return entity;
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey)
	{
		return this.find(entityClass, primaryKey);
	}

	@Override
	public void flush()
	{
		// Do nothing here
	}

	@Override
	public void setFlushMode(FlushModeType flushMode)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");

	}

	@Override
	public FlushModeType getFlushMode()
	{
		return FlushModeType.AUTO;
	}

	@Override
	public void lock(Object entity, LockModeType lockMode)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public void refresh(Object entity)
	{

		if (!helper.isProxy(entity))
		{
			throw new IllegalStateException("The entity " + entity + " is not in 'managed' state");
		}
		else
		{
			entityRefresher.refresh(entity, entityMetaMap);
		}
	}

	@Override
	public void clear()
	{
		//

	}

	@Override
	public boolean contains(Object entity)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public Query createQuery(String qlString)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public Query createNamedQuery(String name)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public Query createNativeQuery(String sqlString)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public Query createNativeQuery(String sqlString, Class resultClass)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	@Override
	public void joinTransaction()
	{
		// Do nothing

	}

	@Override
	public Object getDelegate()
	{
		return this;
	}

	@Override
	public void close()
	{
		// Do nothing

	}

	@Override
	public boolean isOpen()
	{
		return false;
	}

	@Override
	public EntityTransaction getTransaction()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}
}
