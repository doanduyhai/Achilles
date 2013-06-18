package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * CQLPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersistenceContext extends AchillesPersistenceContext
{
	private CQLDaoContext daoContext;
	private CQLAbstractFlushContext<?> flushContext;
	private CQLEntityLoader loader = new CQLEntityLoader();
	private CQLEntityPersister persister = new CQLEntityPersister();
	private CQLEntityMerger merger = new CQLEntityMerger();
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();
	private EntityRefresher<CQLPersistenceContext> refresher;

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext<?> flushContext,
			Class<?> entityClass,
			Object primaryKey, Set<String> entitiesIdentity)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext, entitiesIdentity);
		initCollaborators(daoContext, flushContext);
	}

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext<?> flushContext, Object entity,
			Set<String> entitiesIdentity)
	{
		super(entityMeta, configContext, entity, flushContext, entitiesIdentity);
		initCollaborators(daoContext, flushContext);
	}

	private void initCollaborators(CQLDaoContext daoContext, CQLAbstractFlushContext<?> flushContext)
	{
		this.refresher = new EntityRefresher<CQLPersistenceContext>(loader, proxifier);
		this.daoContext = daoContext;
		this.flushContext = flushContext;
	}

	@Override
	public CQLPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity)
	{
		Validator.validateNotNull(joinEntity, "join entity should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext,
				flushContext.duplicateWithoutTtl(),
				joinEntity, entitiesIdentity);
	}

	@Override
	public CQLPersistenceContext newPersistenceContext(Class<?> entityClass, EntityMeta joinMeta,
			Object joinId)
	{
		Validator.validateNotNull(entityClass, "entityClass should not be null");
		Validator.validateNotNull(joinId, "joinId should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext,
				flushContext.duplicateWithoutTtl(),
				entityClass, joinId, entitiesIdentity);
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

	public void bindForSimpleCounterRemoval(EntityMeta meta, PropertyMeta<?, ?> counterMeta,
			Object primaryKey)
	{
		daoContext.bindForSimpleCounterDelete(this, meta, counterMeta, primaryKey);
	}

	public void pushBoundStatement(BoundStatement boundStatement, ConsistencyLevel writeLevel)
	{
		flushContext.pushBoundStatement(boundStatement, writeLevel);
	}

	public ResultSet executeImmediateWithConsistency(BoundStatement bs,
			ConsistencyLevel readConsistencyLevel)
	{
		return flushContext.executeImmediateWithConsistency(bs, readConsistencyLevel);
	}

	@Override
	public void persist()
	{
		persister.persist(this);
		flush();
	}

	@Override
	public <T> T merge(T entity)
	{
		T merged = merger.merge(this, entity);
		flush();
		return merged;
	}

	@Override
	public void remove()
	{
		persister.remove(this);
		flush();
	}

	@Override
	public <T> T find(Class<T> entityClass)
	{
		T entity = loader.<T> load(this, entityClass);

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
		refresher.refresh(this);
	}

	@Override
	public <T> T initialize(T entity)
	{
		proxifier.ensureProxy(entity);
		final EntityInterceptor<CQLPersistenceContext, T> interceptor = proxifier
				.getInterceptor(entity);
		initializer.initializeEntity(entity, entityMeta, interceptor);
		return entity;
	}
}
