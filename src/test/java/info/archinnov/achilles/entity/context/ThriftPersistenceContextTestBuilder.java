package info.archinnov.achilles.entity.context;

import static org.mockito.Mockito.mock;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

import org.powermock.reflect.Whitebox;

/**
 * ThriftPersistenceContextTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersistenceContextTestBuilder
{
	private EntityMeta entityMeta;
	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap = new HashMap<String, ThriftGenericWideRowDao>();
	private ThriftCounterDao thriftCounterDao;
	private ThriftConsistencyLevelPolicy policy;
	private boolean ensureJoinConsistency;
	private Object entity;
	private Class<?> entityClass;
	private Object primaryKey;
	private ThriftGenericEntityDao entityDao;
	private ThriftGenericWideRowDao wideRowDao;

	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	public static ThriftPersistenceContextTestBuilder context(EntityMeta entityMeta,//
			ThriftCounterDao thriftCounterDao, //
			ThriftConsistencyLevelPolicy policy, //
			Class<?> entityClass, Object primaryKey)
	{
		return new ThriftPersistenceContextTestBuilder(entityMeta, thriftCounterDao, policy,
				entityClass, primaryKey);
	}

	public static ThriftPersistenceContextTestBuilder mockAll(EntityMeta entityMeta,
			Class<?> entityClass, Object primaryKey)
	{
		return new ThriftPersistenceContextTestBuilder(entityMeta, mock(ThriftCounterDao.class),
				mock(ThriftConsistencyLevelPolicy.class), entityClass, primaryKey);
	}

	public ThriftPersistenceContextTestBuilder(EntityMeta entityMeta,
			ThriftCounterDao thriftCounterDao, ThriftConsistencyLevelPolicy policy,
			Class<?> entityClass, Object primaryKey)
	{
		this.entityMeta = entityMeta;
		this.thriftCounterDao = thriftCounterDao;
		this.policy = policy;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
	}

	public ThriftPersistenceContext build()
	{
		ThriftDaoContext thriftDaoContext = new ThriftDaoContext(entityDaosMap,
				columnFamilyDaosMap, thriftCounterDao);
		AchillesConfigurationContext configContext = new AchillesConfigurationContext();
		configContext.setConsistencyPolicy(policy);
		configContext.setEnsureJoinConsistency(ensureJoinConsistency);
		ThriftPersistenceContext context = new ThriftPersistenceContext(//
				entityMeta, //
				configContext, //
				thriftDaoContext, //
				thriftImmediateFlushContext, //
				entityClass, primaryKey);

		context.setEntity(entity);
		Whitebox.setInternalState(context, "entityDao", entityDao);
		Whitebox.setInternalState(context, "wideRowDao", wideRowDao);
		return context;
	}

	public ThriftPersistenceContextTestBuilder entityDaosMap(
			Map<String, ThriftGenericEntityDao> entityDaosMap)
	{
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public ThriftPersistenceContextTestBuilder columnFamilyDaosMap(
			Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap)
	{
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public ThriftPersistenceContextTestBuilder entity(Object entity)
	{
		this.entity = entity;
		return this;
	}

	public ThriftPersistenceContextTestBuilder entityDao(ThriftGenericEntityDao entityDao)
	{
		this.entityDao = entityDao;
		return this;
	}

	public ThriftPersistenceContextTestBuilder columnFamilyDao(
			ThriftGenericWideRowDao columnFamilyDao)
	{
		this.wideRowDao = columnFamilyDao;
		return this;
	}

	public ThriftPersistenceContextTestBuilder thriftImmediateFlushContext(
			ThriftImmediateFlushContext thriftImmediateFlushContext)
	{
		this.thriftImmediateFlushContext = thriftImmediateFlushContext;
		return this;
	}

	public ThriftPersistenceContextTestBuilder ensureJoinConsistency(boolean ensureJoinConsistency)
	{
		this.ensureJoinConsistency = ensureJoinConsistency;
		return this;
	}

}
