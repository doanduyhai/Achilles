package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CQLPreparedStatementBinder;

import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
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
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Session session;

	private CQLPreparedStatementBinder binder = new CQLPreparedStatementBinder();

	public CQLDaoContext(Map<Class<?>, PreparedStatement> insertPSs,
			Map<Class<?>, PreparedStatement> selectForExistenceCheckPSs,
			Map<Class<?>, PreparedStatement> selectEagerPSs,
			Map<Class<?>, Map<String, PreparedStatement>> removePSs, Session session)
	{
		this.insertPSs = insertPSs;
		this.selectForExistenceCheckPSs = selectForExistenceCheckPSs;
		this.selectEagerPSs = selectEagerPSs;
		this.removePSs = removePSs;
		this.session = session;
	}

	public Session getSession()
	{
		return session;
	}

	public BoundStatement bindForInsert(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = insertPSs.get(entityClass);
		return binder.bind(ps, entityMeta, context.getEntity());
	}

	public boolean checkForEntityExistence(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectForExistenceCheckPSs.get(entityClass);
		BoundStatement boundStatement = binder.bind(ps, entityMeta, context.getEntity());
		ResultSet resultSet = session.execute(boundStatement);
		return resultSet.all().size() == 1;
	}

	public BoundStatement bindForRemove(CQLPersistenceContext context, String tableName)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Map<String, PreparedStatement> psMap = removePSs.get(entityClass);
		if (psMap.containsKey(tableName))
		{
			return binder.bind(psMap.get(tableName), entityMeta, context.getEntity());
		}
		else
		{
			throw new AchillesException("Cannot find prepared statement for deletion for table '"
					+ tableName + "'");
		}
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
}
