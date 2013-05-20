package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

	protected ThriftDaoContext thriftDaoContext;

	protected Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>();
	protected boolean hasCustomConsistencyLevels = false;

	protected ThriftAbstractFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy)
	{
		this.thriftDaoContext = thriftDaoContext;
		this.consistencyContext = new ThriftConsistencyContext(policy);
	}

	protected void doFlush()
	{
		log.debug("Execute mutations flush");
		try
		{
			for (Entry<String, Pair<Mutator<Object>, ThriftAbstractDao>> entry : mutatorMap
					.entrySet())
			{
				ThriftAbstractDao dao = entry.getValue().right;
				Mutator<?> mutator = entry.getValue().left;
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

	public Mutator<Object> getEntityMutator(String columnFamilyName)
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(columnFamilyName))
		{
			mutator = mutatorMap.get(columnFamilyName).left;
		}
		else
		{
			ThriftGenericEntityDao entityDao = thriftDaoContext.findEntityDao(columnFamilyName);

			if (entityDao != null)
			{
				mutator = entityDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<Object>, ThriftAbstractDao>(
						mutator, entityDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getWideRowMutator(String columnFamilyName)
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(columnFamilyName))
		{
			mutator = mutatorMap.get(columnFamilyName).left;
		}
		else
		{
			ThriftGenericWideRowDao columnFamilyDao = thriftDaoContext.findWideRowDao(columnFamilyName);

			if (columnFamilyDao != null)
			{
				mutator = columnFamilyDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<Object>, ThriftAbstractDao>(
						mutator, columnFamilyDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getCounterMutator()
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(COUNTER_CF))
		{
			mutator = mutatorMap.get(COUNTER_CF).left;
		}
		else
		{
			ThriftCounterDao thriftCounterDao = thriftDaoContext.getCounterDao();
			mutator = thriftCounterDao.buildMutator();
			mutatorMap.put(COUNTER_CF, new Pair<Mutator<Object>, ThriftAbstractDao>(mutator,
					thriftCounterDao));
		}
		return mutator;
	}

}
