package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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

	public CQLPersistenceContext(EntityMeta entityMeta, AchillesConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext flushContext, Class<?> entityClass,
			Object primaryKey)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext);
		this.daoContext = daoContext;
		this.flushContext = flushContext;
	}

	public CQLPersistenceContext(EntityMeta entityMeta, AchillesConfigurationContext configContext,
			CQLDaoContext daoContext, CQLAbstractFlushContext flushContext, Object entity)
	{
		super(entityMeta, configContext, entity, flushContext);
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
}
