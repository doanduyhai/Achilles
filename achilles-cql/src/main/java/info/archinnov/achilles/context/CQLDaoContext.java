package info.archinnov.achilles.context;

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
	public static final String ACHILLES_COUNTER_TABLE = "achillesCounterTable";
	public static final String ACHILLES_COUNTER_FQCN = "fqcn";
	public static final String ACHILLES_COUNTER_PK = "pk";

	private Map<Class<?>, PreparedStatement> insertPSs;
	private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;
	private Map<Class<?>, PreparedStatement> selectEagerPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Session session;

	private CQLPreparedStatementBinder binder = new CQLPreparedStatementBinder();
	private CacheManager cacheManager = new CacheManager();

	public CQLDaoContext(Map<Class<?>, PreparedStatement> insertPSs,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
			Map<Class<?>, PreparedStatement> selectEagerPSs,
			Map<Class<?>, Map<String, PreparedStatement>> removePSs, Session session)
	{
		this.insertPSs = insertPSs;
		this.dynamicPSCache = dynamicPSCache;
		this.selectEagerPSs = selectEagerPSs;
		this.removePSs = removePSs;
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
		return executeReadWithConsistency(context, ps).size() == 1;
	}

	public Row loadProperty(CQLPersistenceContext context, PropertyMeta<?, ?> pm)
	{
		PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache,
				context, pm);
		List<Row> rows = executeReadWithConsistency(context, ps);
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
					entityMeta, context.getEntity());
			context.pushBoundStatement(bs, writeLevel);
		}
		else
		{
			throw new AchillesException("Cannot find prepared statement for deletion for table '"
					+ tableName + "'");
		}
	}

	public Row eagerLoadEntity(CQLPersistenceContext context)
	{
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectEagerPSs.get(entityClass);

		List<Row> rows = executeReadWithConsistency(context, ps);
		return returnFirstRowOrNull(rows);
	}

	private List<Row> executeReadWithConsistency(CQLPersistenceContext context, PreparedStatement ps)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		BoundStatement boundStatement = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getEntity());

		return context.executeImmediateWithConsistency(session, boundStatement).all();
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

}
