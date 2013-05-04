package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.FlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContext<ID>
{
	private static final Logger log = LoggerFactory.getLogger(PersistenceContext.class);

	private final EntityIntrospector introspector = new EntityIntrospector();
	private final EntityMeta<ID> entityMeta;
	private final DaoContext daoContext;
	private final ConfigurationContext configContext;
	private final AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private GenericEntityDao<ID> entityDao;
	private GenericWideRowDao<ID, ?> wideRowDao;

	// Flush
	private FlushContext flushContext;

	public PersistenceContext(EntityMeta<ID> entityMeta, //
			ConfigurationContext configContext, //
			DaoContext daoContext, //
			FlushContext flushContext, //
			Object entity)
	{
		log.trace("Create new persistence context for instance {} of class {}", entity,
				entityMeta.getClassName());

		this.entityMeta = entityMeta;
		this.daoContext = daoContext;
		this.configContext = configContext;
		this.policy = configContext.getConsistencyPolicy();
		this.flushContext = flushContext;
		this.entity = entity;
		this.entityClass = entity.getClass();

		this.primaryKey = introspector.getKey(entity, entityMeta.getIdMeta());

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");

		this.initDaos();
	}

	public PersistenceContext(EntityMeta<ID> entityMeta, //
			ConfigurationContext configContext, //
			DaoContext daoContext, //
			FlushContext flushContext, //
			Class<?> entityClass, ID primaryKey)
	{
		log.trace("Create new persistence context for instance {} of class {}", entity,
				entityMeta.getClassName());

		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.policy = configContext.getConsistencyPolicy();
		this.daoContext = daoContext;
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
		log.trace("Spawn new persistence context for instance {} of join class {}", joinEntity,
				joinMeta.getClassName());
		return new PersistenceContext<JOIN_ID>(joinMeta, configContext, daoContext, flushContext,
				joinEntity);
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(Class<?> entityClass,
			EntityMeta<JOIN_ID> joinMeta, JOIN_ID joinId)
	{
		log.trace("Spawn new persistence context for primary key {} of join class {}", joinId,
				joinMeta.getClassName());

		return new PersistenceContext<JOIN_ID>(joinMeta, configContext, daoContext, flushContext,
				entityClass, joinId);
	}

	public GenericEntityDao<?> findEntityDao(String columnFamilyName)
	{
		return daoContext.findEntityDao(columnFamilyName);
	}

	public GenericWideRowDao<?, ?> findWideRowDao(String columnFamilyName)
	{
		return daoContext.findWideRowDao(columnFamilyName);
	}

	public CounterDao getCounterDao()
	{
		return daoContext.getCounterDao();
	}

	public boolean isWideRow()
	{
		return this.entityMeta.isWideRow();
	}

	public String getColumnFamilyName()
	{
		return entityMeta.getColumnFamilyName();
	}

	public Mutator<ID> getCurrentColumnFamilyMutator()
	{
		return flushContext.getWideRowMutator(entityMeta.getColumnFamilyName());
	}

	public Mutator<ID> getWideRowMutator(String columnFamilyName)
	{
		return flushContext.getWideRowMutator(columnFamilyName);
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
		return flushContext.type() == FlushType.BATCH;
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

	public Object getEntity()
	{
		return entity;
	}

	public void setEntity(Object entity)
	{
		this.entity = entity;
	}

	public GenericEntityDao<ID> getEntityDao()
	{
		return entityDao;
	}

	public GenericWideRowDao<ID, ?> getColumnFamilyDao()
	{
		return wideRowDao;
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

	public ConfigurationContext getConfigContext()
	{
		return configContext;
	}

	public AchillesConfigurableConsistencyLevelPolicy getPolicy()
	{
		return policy;
	}

	@SuppressWarnings("unchecked")
	private void initDaos()
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		if (entityMeta.isWideRow())
		{
			this.wideRowDao = (GenericWideRowDao<ID, ?>) daoContext
					.findWideRowDao(columnFamilyName);
		}
		else
		{
			this.entityDao = (GenericEntityDao<ID>) daoContext.findEntityDao(columnFamilyName);
		}
	}
}
