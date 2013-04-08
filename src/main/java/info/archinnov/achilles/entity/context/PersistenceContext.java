package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.FlushContext.BatchType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * PersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContext<ID>
{
	private final EntityIntrospector introspector = new EntityIntrospector();
	private final EntityMeta<ID> entityMeta;
	private final Map<String, GenericEntityDao<?>> entityDaosMap;
	private final Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	private final CounterDao counterDao;
	private final AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private GenericEntityDao<ID> entityDao;
	private GenericColumnFamilyDao<ID, ?> columnFamilyDao;

	// Flush
	private FlushContext flushContext;

	public PersistenceContext(EntityMeta<ID> entityMeta,
			Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, //
			CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy policy, //
			FlushContext flushContext, //
			Object entity)
	{
		this.entityMeta = entityMeta;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.policy = policy;
		this.flushContext = flushContext;
		this.entity = entity;
		this.entityClass = entity.getClass();

		this.primaryKey = introspector.getKey(entity, entityMeta.getIdMeta());

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");

		this.initDaos();
	}

	public PersistenceContext(EntityMeta<ID> entityMeta,
			Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy policy, //
			FlushContext flushContext, //
			Class<?> entityClass, ID primaryKey)
	{
		this.entityMeta = entityMeta;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.policy = policy;
		this.flushContext = flushContext;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");

		this.initDaos();
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(
			EntityMeta<JOIN_ID> joinMeta, Object joinEntity)
	{
		return new PersistenceContext<JOIN_ID>(joinMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, policy, flushContext, joinEntity);
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(Class<?> entityClass,
			EntityMeta<JOIN_ID> joinMeta, JOIN_ID joinId)
	{
		return new PersistenceContext<JOIN_ID>(joinMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, policy, flushContext, entityClass, joinId);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID> GenericEntityDao<JOIN_ID> findEntityDao(String columnFamilyName)
	{
		return (GenericEntityDao<JOIN_ID>) entityDaosMap.get(columnFamilyName);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, V> GenericColumnFamilyDao<JOIN_ID, V> findColumnFamilyDao(
			String columnFamilyName)
	{
		return (GenericColumnFamilyDao<JOIN_ID, V>) columnFamilyDaosMap.get(columnFamilyName);
	}

	public boolean isDirectColumnFamilyMapping()
	{
		return this.entityMeta.isColumnFamilyDirectMapping();
	}

	public String getColumnFamilyName()
	{
		return entityMeta.getColumnFamilyName();
	}

	public Mutator<ID> getCurrentColumnFamilyMutator()
	{
		return flushContext.getColumnFamilyMutator(entityMeta.getColumnFamilyName());
	}

	public Mutator<ID> getColumnFamilyMutator(String columnFamilyName)
	{
		return flushContext.getColumnFamilyMutator(columnFamilyName);
	}

	public Mutator<ID> getCurrentEntityMutator()
	{
		return flushContext.getEntityMutator(entityMeta.getColumnFamilyName());
	}

	public Mutator<ID> getEntityMutator(String columnFamilyName)
	{
		return flushContext.getEntityMutator(columnFamilyName);
	}

	public Mutator<Composite> getCounterMutator()
	{
		return flushContext.getCounterMutator();
	}

	public boolean isBatchMode()
	{
		return flushContext.type() == BatchType.BATCH;
	}

	public void flush()
	{
		flushContext.flush();
	}

	public void endBatch()
	{
		flushContext.endBatch();
	}

	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		flushContext.setReadConsistencyLevel(readLevel);
	}

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		flushContext.setWriteConsistencyLevel(writeLevel);
	}

	public void reinitConsistencyLevels()
	{
		flushContext.reinitConsistencyLevels();
	}

	public void cleanUpFlushContext()
	{
		flushContext.cleanUp();
	}

	public EntityMeta<ID> getEntityMeta()
	{
		return entityMeta;
	}

	public Map<String, GenericEntityDao<?>> getEntityDaosMap()
	{
		return entityDaosMap;
	}

	public Map<String, GenericColumnFamilyDao<?, ?>> getColumnFamilyDaosMap()
	{
		return columnFamilyDaosMap;
	}

	public Object getEntity()
	{
		return entity;
	}

	public void setEntity(Object entity)
	{
		this.entity = entity;
	}

	public CounterDao getCounterDao()
	{
		return counterDao;
	}

	public GenericEntityDao<ID> getEntityDao()
	{
		return entityDao;
	}

	public GenericColumnFamilyDao<ID, ?> getColumnFamilyDao()
	{
		return columnFamilyDao;
	}

	public Class<?> getEntityClass()
	{
		return entityClass;
	}

	public ID getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(ID primaryKey)
	{
		this.primaryKey = primaryKey;
	}

	public AchillesConfigurableConsistencyLevelPolicy getPolicy()
	{
		return policy;
	}

	@SuppressWarnings("unchecked")
	private void initDaos()
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			this.columnFamilyDao = (GenericColumnFamilyDao<ID, ?>) columnFamilyDaosMap
					.get(columnFamilyName);
		}
		else
		{
			this.entityDao = (GenericEntityDao<ID>) entityDaosMap.get(columnFamilyName);
		}
	}
}
