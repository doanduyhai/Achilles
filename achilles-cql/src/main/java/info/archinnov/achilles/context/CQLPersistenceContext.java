package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;

/**
 * CQLPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersistenceContext extends AchillesPersistenceContext
{

	private CQLDaoContext daoContext;
	private CQLAbstractFlushContext flushContext;
	private CQLEntityLoader loader = new CQLEntityLoader();
	private CQLEntityPersister persister = new CQLEntityPersister();
	private CQLEntityMerger merger = new CQLEntityMerger();
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();
	private EntityRefresher<CQLPersistenceContext> refresher;

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext flushContext, Class<?> entityClass,
			Object primaryKey)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext);
		initCollaborators(daoContext, flushContext);
	}

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext flushContext, Object entity)
	{
		super(entityMeta, configContext, entity, flushContext);
		initCollaborators(daoContext, flushContext);
	}

	private void initCollaborators(CQLDaoContext daoContext, CQLAbstractFlushContext flushContext)
	{
		this.refresher = new EntityRefresher<CQLPersistenceContext>(loader, proxifier);
		this.daoContext = daoContext;
		this.flushContext = flushContext;
	}

	@Override
	public CQLPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity)
	{
		Validator.validateNotNull(joinEntity, "join entity should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext, flushContext,
				joinEntity);
	}

	@Override
	public CQLPersistenceContext newPersistenceContext(Class<?> entityClass, EntityMeta joinMeta,
			Object joinId)
	{
		Validator.validateNotNull(entityClass, "entityClass should not be null");
		Validator.validateNotNull(joinId, "joinId should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext, flushContext,
				entityClass, joinId);
	}

	public boolean checkForEntityExistence()
	{
		return daoContext.checkForEntityExistence(this);
	}

	public Row eagerLoadEntity()
	{
		return daoContext.eagerLoadEntity(this);
	}

	public Row loadProperty(PropertyMeta<?, ?> pm)
	{
		return daoContext.loadProperty(this, pm);
	}

	public void bindForInsert()
	{
		daoContext.bindForInsert(this);
	}

	public void bindForUpdate(List<PropertyMeta<?, ?>> pms)
	{
		daoContext.bindForUpdate(this, pms);
	}

	public void bindForRemoval(String tableName, ConsistencyLevel writeLevel)
	{
		daoContext.bindForRemoval(this, tableName, writeLevel);
	}

	public void pushBoundStatement(BoundStatement boundStatement, ConsistencyLevel writeLevel)
	{
		flushContext.pushBoundStatement(boundStatement, writeLevel);
	}

	public ResultSet executeImmediateWithConsistency(Session session, BoundStatement bs)
	{
		return flushContext.executeImmediateWithConsistency(session, bs, entityMeta);
	}

	@Override
	public void persist(Optional<ConsistencyLevel> writeLevelO)
	{
		flushContext.setWriteConsistencyLevel(writeLevelO.orNull());
		persister.persist(this);
		flush();
	}

	@Override
	public <T> T merge(T entity, Optional<ConsistencyLevel> writeLevelO)
	{
		flushContext.setWriteConsistencyLevel(writeLevelO.orNull());
		T merged = merger.merge(this, entity);
		flush();
		return merged;
	}

	@Override
	public void remove(Optional<ConsistencyLevel> writeLevelO)
	{
		flushContext.setWriteConsistencyLevel(writeLevelO.orNull());
		persister.remove(this);
		flush();
	}

	@Override
	public <T> T find(Class<T> entityClass, Optional<ConsistencyLevel> readLevelO)
	{
		flushContext.setReadConsistencyLevel(readLevelO.orNull());
		T entity = loader.<T> load(this, entityClass);

		if (entity != null)
		{
			entity = proxifier.buildProxy(entity, this);
		}
		return entity;
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Optional<ConsistencyLevel> readLevelO)
	{
		setLoadEagerFields(false);
		return find(entityClass, readLevelO);
	}

	@Override
	public void refresh(Optional<ConsistencyLevel> readLevelO)
	{
		flushContext.setReadConsistencyLevel(readLevelO.orNull());
		refresher.refresh(this);
	}
}
