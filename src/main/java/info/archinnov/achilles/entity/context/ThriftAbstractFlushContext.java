package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractFlushContext extends AchillesFlushContext
{
	protected static final Logger log = LoggerFactory.getLogger(ThriftAbstractFlushContext.class);

	protected DaoContext daoContext;

	protected Map<String, Pair<Mutator<?>, ThriftAbstractDao<?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, ThriftAbstractDao<?, ?>>>();
	protected boolean hasCustomConsistencyLevels = false;

	protected ThriftAbstractFlushContext(DaoContext daoContext,
			AchillesConsistencyLevelPolicy policy)
	{
		this.daoContext = daoContext;
		this.consistencyContext = new ThriftConsistencyContext(policy);
	}

	@SuppressWarnings("unchecked")
	protected <ID> void doFlush()
	{
		log.debug("Execute mutations flush");
		try
		{
			for (Entry<String, Pair<Mutator<?>, ThriftAbstractDao<?, ?>>> entry : mutatorMap
					.entrySet())
			{
				ThriftAbstractDao<ID, ?> dao = (ThriftAbstractDao<ID, ?>) entry.getValue().right;
				Mutator<ID> mutator = (Mutator<ID>) entry.getValue().left;
				dao.executeMutator(mutator);
			}
		}
		finally
		{
			cleanUp();
		}
	}

	@Override
	public void cleanUp()
	{
		log.debug("Cleaning up flush context");
		consistencyContext.reinitConsistencyLevels();
		hasCustomConsistencyLevels = false;
		mutatorMap.clear();
	}

	@Override
	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setWriteConsistencyLevel(writeLevel);
	}

	@Override
	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setReadConsistencyLevel(readLevel);
	}

	@Override
	public void reinitConsistencyLevels()
	{
		if (!hasCustomConsistencyLevels)
		{
			consistencyContext.reinitConsistencyLevels();
		}
	}

	@SuppressWarnings("unchecked")
	public <ID> Mutator<ID> getEntityMutator(String columnFamilyName)
	{
		Mutator<ID> mutator = null;
		if (mutatorMap.containsKey(columnFamilyName))
		{
			mutator = (Mutator<ID>) mutatorMap.get(columnFamilyName).left;
		}
		else
		{
			ThriftGenericEntityDao<ID> entityDao = (ThriftGenericEntityDao<ID>) daoContext
					.findEntityDao(columnFamilyName);

			if (entityDao != null)
			{
				mutator = entityDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, ThriftAbstractDao<?, ?>>(
						mutator, entityDao));
			}
		}
		return mutator;
	}

	@SuppressWarnings("unchecked")
	public <ID> Mutator<ID> getWideRowMutator(String columnFamilyName)
	{
		Mutator<ID> mutator = null;
		if (mutatorMap.containsKey(columnFamilyName))
		{
			mutator = (Mutator<ID>) mutatorMap.get(columnFamilyName).left;
		}
		else
		{
			ThriftGenericWideRowDao<ID, ?> columnFamilyDao = (ThriftGenericWideRowDao<ID, ?>) daoContext
					.findWideRowDao(columnFamilyName);

			if (columnFamilyDao != null)
			{
				mutator = columnFamilyDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, ThriftAbstractDao<?, ?>>(
						mutator, columnFamilyDao));
			}
		}
		return mutator;
	}

	@SuppressWarnings("unchecked")
	public Mutator<Composite> getCounterMutator()
	{
		Mutator<Composite> mutator = null;
		if (mutatorMap.containsKey(COUNTER_CF))
		{
			mutator = (Mutator<Composite>) mutatorMap.get(COUNTER_CF).left;
		}
		else
		{
			ThriftCounterDao thriftCounterDao = daoContext.getCounterDao();
			mutator = thriftCounterDao.buildMutator();
			mutatorMap.put(COUNTER_CF, new Pair<Mutator<?>, ThriftAbstractDao<?, ?>>(mutator,
					thriftCounterDao));
		}
		return mutator;
	}

}
