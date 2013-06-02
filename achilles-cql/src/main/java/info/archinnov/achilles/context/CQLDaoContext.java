package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CQLPreparedStatementBinder;

import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
	private Map<Class<?>, PreparedStatement> selectForExistenceCheckPSs;
	private Map<Class<?>, PreparedStatement> selectEagerPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> selectFieldPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Session session;

	private CQLPreparedStatementBinder binder = new CQLPreparedStatementBinder();

	public CQLDaoContext(Map<Class<?>, PreparedStatement> insertPSs,
			Map<Class<?>, PreparedStatement> selectForExistenceCheckPSs,
			Map<Class<?>, Map<String, PreparedStatement>> selectFieldPSs,
			Map<Class<?>, PreparedStatement> selectEagerPSs,
			Map<Class<?>, Map<String, PreparedStatement>> removePSs, Session session)
	{
		this.insertPSs = insertPSs;
		this.selectForExistenceCheckPSs = selectForExistenceCheckPSs;
		this.selectFieldPSs = selectFieldPSs;
		this.selectEagerPSs = selectEagerPSs;
		this.removePSs = removePSs;
		this.session = session;
	}

	public BoundStatement bindForInsert(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = insertPSs.get(entityClass);
		return binder.bindForInsert(ps, entityMeta, context.getEntity());
	}

	public boolean checkForEntityExistence(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectForExistenceCheckPSs.get(entityClass);
		BoundStatement boundStatement = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getEntity());
		ResultSet resultSet = context.getFlushContext().executeImmediateWithConsistency(session,
				boundStatement, entityMeta);
		return resultSet.all().size() == 1;
	}

	public BoundStatement bindForRemoval(CQLPersistenceContext context, String tableName)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Map<String, PreparedStatement> psMap = removePSs.get(entityClass);
		if (psMap.containsKey(tableName))
		{
			return binder.bindStatementWithOnlyPKInWhereClause(psMap.get(tableName), entityMeta,
					context.getEntity());
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

		return executeReadWithConsistency(context, ps);
	}

	public Row loadProperty(CQLPersistenceContext context, PropertyMeta<?, ?> pm)
	{
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectFieldPSs.get(entityClass).get(pm.getPropertyName());

		return executeReadWithConsistency(context, ps);
	}

	private Row executeReadWithConsistency(CQLPersistenceContext context, PreparedStatement ps)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		BoundStatement boundStatement = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getEntity());

		return context
				.getFlushContext()
				.executeImmediateWithConsistency(session, boundStatement, entityMeta)
				.one();
	}

	public Session getSession()
	{
		return session;
	}

	public Map<Class<?>, PreparedStatement> getInsertPSs()
	{
		return insertPSs;
	}

	public Map<Class<?>, PreparedStatement> getSelectForExistenceCheckPSs()
	{
		return selectForExistenceCheckPSs;
	}

	public Map<Class<?>, PreparedStatement> getSelectEagerPSs()
	{
		return selectEagerPSs;
	}

	public Map<Class<?>, Map<String, PreparedStatement>> getRemovePSs()
	{
		return removePSs;
	}

	public Map<Class<?>, Map<String, PreparedStatement>> getSelectFieldPSs()
	{
		return selectFieldPSs;
	}
}
