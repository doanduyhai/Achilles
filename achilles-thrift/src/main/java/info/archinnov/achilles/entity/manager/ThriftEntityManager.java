package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityManager
 * 
 * Thrift-based Entity Manager for Achilles. This entity manager is perfectly thread-safe and
 * 
 * can be used as a singleton. Entity state is stored in proxy object, which is obviously not
 * 
 * thread-safe.
 * 
 * Internally the ThriftEntityManager relies on Hector API for common operations
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManager extends AchillesEntityManager<ThriftPersistenceContext>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityManager.class);

	protected ThriftDaoContext thriftDaoContext;

	ThriftEntityManager(AchillesEntityManagerFactory entityManagerFactory,
			Map<Class<?>, EntityMeta> entityMetaMap, //
			ThriftDaoContext thriftDaoContext, //
			ConfigurationContext configContext)
	{
		super(entityManagerFactory, entityMetaMap, configContext);
		this.thriftDaoContext = thriftDaoContext;
		super.proxifier = new ThriftEntityProxifier();
		super.entityValidator = new EntityValidator<ThriftPersistenceContext>(super.proxifier);
	}

	/**
	 * Create a new state-full EntityManager for batch handling <br/>
	 * <br/>
	 * 
	 * <strong>WARNING : This EntityManager is state-full and not thread-safe. In case of exception, you MUST not re-use it but create another one</strong>
	 * 
	 * @return a new state-full EntityManager
	 */
	public ThriftBatchingEntityManager batchingEntityManager()
	{
		return new ThriftBatchingEntityManager(entityManagerFactory, entityMetaMap,
				thriftDaoContext, configContext);
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(Class<?> entityClass,
			Object primaryKey)
	{
		log.trace("Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);

		EntityMeta entityMeta = this.entityMetaMap.get(entityClass);
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, new ThriftImmediateFlushContext(thriftDaoContext,
						consistencyPolicy), entityClass, primaryKey);
		return context;
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(Object entity)
	{
		log.trace("Initializing new persistence context for entity {}", entity);

		EntityMeta entityMeta = this.entityMetaMap.get(proxifier.deriveBaseClass(entity));
		return new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				new ThriftImmediateFlushContext(thriftDaoContext, consistencyPolicy), entity);
	}

	protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext)
	{
		this.thriftDaoContext = thriftDaoContext;
	}
}
