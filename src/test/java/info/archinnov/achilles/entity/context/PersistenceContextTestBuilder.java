package info.archinnov.achilles.entity.context;

import static org.mockito.Mockito.mock;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

import org.powermock.reflect.Whitebox;

/**
 * PersistenceContextTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContextTestBuilder<ID>
{
	private EntityMeta<ID> entityMeta;
	private Map<String, ThriftGenericEntityDao<?>> entityDaosMap = new HashMap<String, ThriftGenericEntityDao<?>>();
	private Map<String, ThriftGenericWideRowDao<?, ?>> columnFamilyDaosMap = new HashMap<String, ThriftGenericWideRowDao<?, ?>>();
	private ThriftCounterDao thriftCounterDao;
	private ThriftConsistencyLevelPolicy policy;
	private boolean ensureJoinConsistency;
	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private ThriftGenericEntityDao<ID> entityDao;
	private ThriftGenericWideRowDao<ID, ?> wideRowDao;

	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	public static <ID, T> PersistenceContextTestBuilder<ID> context(EntityMeta<ID> entityMeta,//
			ThriftCounterDao thriftCounterDao, //
			ThriftConsistencyLevelPolicy policy, //
			Class<T> entityClass, ID primaryKey)
	{
		return new PersistenceContextTestBuilder<ID>(entityMeta, thriftCounterDao, policy, entityClass,
				primaryKey);
	}

	public static <ID, T> PersistenceContextTestBuilder<ID> mockAll(EntityMeta<ID> entityMeta,
			Class<T> entityClass, ID primaryKey)
	{
		return new PersistenceContextTestBuilder<ID>(entityMeta, mock(ThriftCounterDao.class),
				mock(ThriftConsistencyLevelPolicy.class), entityClass, primaryKey);
	}

	public PersistenceContextTestBuilder(EntityMeta<ID> entityMeta, ThriftCounterDao thriftCounterDao,
			ThriftConsistencyLevelPolicy policy, Class<?> entityClass, ID primaryKey)
	{
		this.entityMeta = entityMeta;
		this.thriftCounterDao = thriftCounterDao;
		this.policy = policy;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
	}

	public ThriftPersistenceContext<ID> build()
	{
		DaoContext daoContext = new DaoContext(entityDaosMap, columnFamilyDaosMap, thriftCounterDao);
		AchillesConfigurationContext configContext = new AchillesConfigurationContext();
		configContext.setConsistencyPolicy(policy);
		configContext.setEnsureJoinConsistency(ensureJoinConsistency);
		ThriftPersistenceContext<ID> context = new ThriftPersistenceContext<ID>(//
				entityMeta, //
				configContext, //
				daoContext, //
				thriftImmediateFlushContext, //
				entityClass, primaryKey);

		context.setEntity(entity);
		Whitebox.setInternalState(context, "entityDao", entityDao);
		Whitebox.setInternalState(context, "wideRowDao", wideRowDao);
		return context;
	}

	public PersistenceContextTestBuilder<ID> entityDaosMap(
			Map<String, ThriftGenericEntityDao<?>> entityDaosMap)
	{
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDaosMap(
			Map<String, ThriftGenericWideRowDao<?, ?>> columnFamilyDaosMap)
	{
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entity(Object entity)
	{
		this.entity = entity;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entityDao(ThriftGenericEntityDao<ID> entityDao)
	{
		this.entityDao = entityDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDao(
			ThriftGenericWideRowDao<ID, ?> columnFamilyDao)
	{
		this.wideRowDao = columnFamilyDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> thriftImmediateFlushContext(
			ThriftImmediateFlushContext thriftImmediateFlushContext)
	{
		this.thriftImmediateFlushContext = thriftImmediateFlushContext;
		return this;
	}

	public PersistenceContextTestBuilder<ID> ensureJoinConsistency(boolean ensureJoinConsistency)
	{
		this.ensureJoinConsistency = ensureJoinConsistency;
		return this;
	}

}
