package info.archinnov.achilles.context;

import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.CQLPreparedStatementBinder;
import info.archinnov.achilles.statement.cache.CacheManager;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;

/**
 * CQLDaoContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLDaoContext
{
	private Map<Class<?>, PreparedStatement> insertPSs;
	private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;
	private Map<Class<?>, PreparedStatement> selectEagerPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;
	private Session session;

	private CQLPreparedStatementBinder binder = new CQLPreparedStatementBinder();
	private CacheManager cacheManager = new CacheManager();

	public CQLDaoContext(Map<Class<?>, PreparedStatement> insertPSs,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
			Map<Class<?>, PreparedStatement> selectEagerPSs,
			Map<Class<?>, Map<String, PreparedStatement>> removePSs,
			Map<CQLQueryType, PreparedStatement> counterQueryMap, Session session)
	{
		this.insertPSs = insertPSs;
		this.dynamicPSCache = dynamicPSCache;
		this.selectEagerPSs = selectEagerPSs;
		this.removePSs = removePSs;
		this.counterQueryMap = counterQueryMap;
		this.session = session;
	}

	public void bindForInsert(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = insertPSs.get(entityClass);
		BoundStatement bs = binder.bindForInsert(ps, entityMeta, context.getEntity());
		context.pushBoundStatement(bs, entityMeta.getWriteConsistencyLevel());
	}

	public void bindForUpdate(CQLPersistenceContext context, List<PropertyMeta<?, ?>> pms)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		PreparedStatement ps = cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache,
				context, pms);
		BoundStatement bs = binder.bindForUpdate(ps, entityMeta, pms, context.getEntity());
		context.pushBoundStatement(bs, entityMeta.getWriteConsistencyLevel());
	}

	public boolean checkForEntityExistence(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache,
				context, entityMeta.getIdMeta());
		return executeReadWithConsistency(context, ps, entityMeta.getReadConsistencyLevel()).size() == 1;
	}

	public Row loadProperty(CQLPersistenceContext context, PropertyMeta<?, ?> pm)
	{
		PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache,
				context, pm);
		List<Row> rows = executeReadWithConsistency(context, ps, pm.getReadConsistencyLevel());
		return returnFirstRowOrNull(rows);
	}

	public void bindForRemoval(CQLPersistenceContext context, String tableName,
			ConsistencyLevel writeLevel)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Map<String, PreparedStatement> psMap = removePSs.get(entityClass);
		if (psMap.containsKey(tableName))
		{

			BoundStatement bs = binder.bindStatementWithOnlyPKInWhereClause(psMap.get(tableName),
					entityMeta, context.getPrimaryKey());
			context.pushBoundStatement(bs, writeLevel);
		}
		else
		{
			throw new AchillesException("Cannot find prepared statement for deletion for table '"
					+ tableName + "'");
		}
	}

	public void bindForSimpleCounterIncrement(CQLPersistenceContext context, EntityMeta meta,
			PropertyMeta<?, ?> counterMeta, Object primaryKey, Long increment)
	{
		PreparedStatement ps = counterQueryMap.get(INCR);
		BoundStatement bs = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				primaryKey, increment);
		context.pushBoundStatement(bs, counterMeta.getWriteConsistencyLevel());
	}

	public void bindForSimpleCounterDecrement(CQLPersistenceContext context, EntityMeta meta,
			PropertyMeta<?, ?> counterMeta, Object primaryKey, Long decrement)
	{
		PreparedStatement ps = counterQueryMap.get(DECR);
		BoundStatement bs = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				primaryKey, decrement);
		context.pushBoundStatement(bs, counterMeta.getWriteConsistencyLevel());
	}

	public Row bindForSimpleCounterSelect(CQLPersistenceContext context, EntityMeta meta,
			PropertyMeta<?, ?> counterMeta, Object primaryKey)
	{
		PreparedStatement ps = counterQueryMap.get(SELECT);
		BoundStatement bs = binder.bindForSimpleCounterSelect(ps, meta, counterMeta, primaryKey);
		ResultSet resultSet = context.executeImmediateWithConsistency(bs,
				counterMeta.getWriteConsistencyLevel());

		return returnFirstRowOrNull(resultSet.all());
	}

	public void bindForSimpleCounterDelete(CQLPersistenceContext context, EntityMeta meta,
			PropertyMeta<?, ?> counterMeta, Object primaryKey)
	{
		PreparedStatement ps = counterQueryMap.get(DELETE);
		BoundStatement bs = binder.bindForSimpleCounterDelete(ps, meta, counterMeta, primaryKey);
		context.pushBoundStatement(bs, counterMeta.getWriteConsistencyLevel());
	}

	public Row eagerLoadEntity(CQLPersistenceContext context)
	{
		EntityMeta meta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectEagerPSs.get(entityClass);

		List<Row> rows = executeReadWithConsistency(context, ps, meta.getReadConsistencyLevel());
		return returnFirstRowOrNull(rows);
	}

	private List<Row> executeReadWithConsistency(CQLPersistenceContext context,
			PreparedStatement ps, ConsistencyLevel readLevel)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		BoundStatement boundStatement = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getPrimaryKey());

		return context.executeImmediateWithConsistency(boundStatement, readLevel).all();
	}

	private Row returnFirstRowOrNull(List<Row> rows)
	{
		if (rows.isEmpty())
		{
			return null;
		}
		else
		{
			return rows.get(0);
		}
	}

	public ResultSet execute(Query query)
	{
		return session.execute(query);
	}
}
