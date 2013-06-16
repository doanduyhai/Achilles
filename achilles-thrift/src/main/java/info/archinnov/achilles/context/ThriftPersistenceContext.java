package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Set;

import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersistenceContext extends AchillesPersistenceContext
{
	private static final Logger log = LoggerFactory.getLogger(ThriftPersistenceContext.class);

	private ThriftEntityPersister persister = new ThriftEntityPersister();
	private ThriftEntityLoader loader = new ThriftEntityLoader();
	private ThriftEntityMerger merger = new ThriftEntityMerger();
	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	private EntityRefresher<ThriftPersistenceContext> refresher;

	private ThriftDaoContext thriftDaoContext;
	private ThriftGenericEntityDao entityDao;
	private ThriftGenericWideRowDao wideRowDao;
	private ThriftAbstractFlushContext thriftFlushContext;
	private AchillesConsistencyLevelPolicy policy;

	public ThriftPersistenceContext(EntityMeta entityMeta, //
			ConfigurationContext configContext, //
			ThriftDaoContext thriftDaoContext, //
			ThriftAbstractFlushContext flushContext, //
			Object entity, Set<String> entitiesIdentity)
	{
		super(entityMeta, configContext, entity, flushContext, entitiesIdentity);
		log.trace("Create new persistence context for instance {} of class {}", entity,
				entityMeta.getClassName());

		initCollaborators(thriftDaoContext, flushContext);
		initDaos();
	}

	public ThriftPersistenceContext(EntityMeta entityMeta, //
			ConfigurationContext configContext, //
			ThriftDaoContext thriftDaoContext, //
			ThriftAbstractFlushContext flushContext, //
			Class<?> entityClass, Object primaryKey, Set<String> entitiesIdentity)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext, entitiesIdentity);
		log.trace("Create new persistence context for instance {} of class {}", entity,
				entityClass.getCanonicalName());

		initCollaborators(thriftDaoContext, flushContext);
		initDaos();
	}

	private void initCollaborators(ThriftDaoContext thriftDaoContext, //
			ThriftAbstractFlushContext flushContext)
	{
		refresher = new EntityRefresher<ThriftPersistenceContext>(loader, proxifier);
		policy = configContext.getConsistencyPolicy();
		this.thriftDaoContext = thriftDaoContext;
		this.thriftFlushContext = flushContext;
	}

	private void initDaos()
	{
		String tableName = entityMeta.getTableName();
		if (entityMeta.isWideRow())
		{
			this.wideRowDao = thriftDaoContext.findWideRowDao(tableName);
		}
		else
		{
			this.entityDao = thriftDaoContext.findEntityDao(tableName);
		}
	}

	@Override
	public ThriftPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity)
	{
		log.trace("Spawn new persistence context for instance {} of join class {}", joinEntity,
				joinMeta.getClassName());
		return new ThriftPersistenceContext(joinMeta, configContext, thriftDaoContext,
				thriftFlushContext, joinEntity, entitiesIdentity);
	}

	@Override
	public ThriftPersistenceContext newPersistenceContext(Class<?> entityClass,
			EntityMeta joinMeta, Object joinId)
	{
		log.trace("Spawn new persistence context for primary key {} of join class {}", joinId,
				joinMeta.getClassName());

		return new ThriftPersistenceContext(joinMeta, configContext, thriftDaoContext,
				thriftFlushContext, entityClass, joinId, entitiesIdentity);
	}

	@Override
	public void persist()
	{
		thriftFlushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
				new SafeExecutionContext<Void>()
				{
					@Override
					public Void execute()
					{
						persister.persist(ThriftPersistenceContext.this);
						flush();
						return null;
					}
				});
	}

	@Override
	public <T> T merge(final T entity)
	{

		return thriftFlushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
				new SafeExecutionContext<T>()
				{
					@Override
					public T execute()
					{
						T merged = merger.<T> merge(ThriftPersistenceContext.this, entity);
						flush();
						return merged;
					}
				});

	}

	@Override
	public void remove()
	{
		thriftFlushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
				new SafeExecutionContext<Void>()
				{
					@Override
					public Void execute()
					{
						persister.remove(ThriftPersistenceContext.this);
						flush();
						return null;
					}
				});
	}

	@Override
	public <T> T find(final Class<T> entityClass)
	{
		T entity = thriftFlushContext.getConsistencyContext().executeWithReadConsistencyLevel(
				new SafeExecutionContext<T>()
				{
					@Override
					public T execute()
					{
						return loader.<T> load(ThriftPersistenceContext.this, entityClass);
					}
				});

		if (entity != null)
		{
			entity = proxifier.buildProxy(entity, this);
		}
		return entity;
	}

	@Override
	public <T> T getReference(Class<T> entityClass)
	{
		setLoadEagerFields(false);
		return find(entityClass);
	}

	@Override
	public void refresh()
	{
		thriftFlushContext.getConsistencyContext().executeWithReadConsistencyLevel(
				new SafeExecutionContext<Void>()
				{
					@Override
					public Void execute()
					{
						refresher.refresh(ThriftPersistenceContext.this);
						return null;
					}
				});

	}

	public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context,
			ConsistencyLevel readLevel)
	{
		return thriftFlushContext.getConsistencyContext().executeWithReadConsistencyLevel(context,
				readLevel);
	}

	public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context,
			ConsistencyLevel writeLevel)
	{
		return thriftFlushContext.getConsistencyContext().executeWithWriteConsistencyLevel(context,
				writeLevel);
	}

	public ThriftGenericEntityDao findEntityDao(String tableName)
	{
		return thriftDaoContext.findEntityDao(tableName);
	}

	public ThriftGenericWideRowDao findWideRowDao(String tableName)
	{
		return thriftDaoContext.findWideRowDao(tableName);
	}

	public ThriftCounterDao getCounterDao()
	{
		return thriftDaoContext.getCounterDao();
	}

	public Mutator<Object> getEntityMutator(String tableName)
	{
		return thriftFlushContext.getEntityMutator(tableName);
	}

	public Mutator<Object> getWideRowMutator(String tableName)
	{
		return thriftFlushContext.getWideRowMutator(tableName);
	}

	public Mutator<Object> getCounterMutator()
	{
		return thriftFlushContext.getCounterMutator();
	}

	public ThriftGenericEntityDao getEntityDao()
	{
		return entityDao;
	}

	public ThriftGenericWideRowDao getWideRowDao()
	{
		return wideRowDao;
	}

	public AchillesConsistencyLevelPolicy getPolicy()
	{
		return policy;
	}

}
