package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * ThriftAbstractFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractFlushContext extends AchillesFlushContext
{
	protected static final Logger log = LoggerFactory.getLogger(ThriftAbstractFlushContext.class);

	protected ThriftDaoContext thriftDaoContext;
	protected ThriftConsistencyContext consistencyContext;

	protected Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>();
	protected boolean hasCustomConsistencyLevels = false;

	protected ThriftAbstractFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy, Optional<ConsistencyLevel> readLevelO,
			Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		super(ttlO);
		this.thriftDaoContext = thriftDaoContext;
		this.consistencyContext = new ThriftConsistencyContext(policy, readLevelO, writeLevelO);
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
	public void setWriteConsistencyLevel(Optional<ConsistencyLevel> writeLevelO)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setWriteConsistencyLevel(writeLevelO);
	}

	@Override
	public void setReadConsistencyLevel(Optional<ConsistencyLevel> readLevelO)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setReadConsistencyLevel(readLevelO);
	}

	@Override
	public void reinitConsistencyLevels()
	{
		if (!hasCustomConsistencyLevels)
		{
			consistencyContext.reinitConsistencyLevels();
		}
	}

	public Mutator<Object> getEntityMutator(String tableName)
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(tableName))
		{
			mutator = mutatorMap.get(tableName).left;
		}
		else
		{
			ThriftGenericEntityDao entityDao = thriftDaoContext.findEntityDao(tableName);

			if (entityDao != null)
			{
				mutator = entityDao.buildMutator();
				mutatorMap.put(tableName, new Pair<Mutator<Object>, ThriftAbstractDao>(mutator,
						entityDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getWideRowMutator(String tableName)
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(tableName))
		{
			mutator = mutatorMap.get(tableName).left;
		}
		else
		{
			ThriftGenericWideRowDao columnFamilyDao = thriftDaoContext.findWideRowDao(tableName);

			if (columnFamilyDao != null)
			{
				mutator = columnFamilyDao.buildMutator();
				mutatorMap.put(tableName, new Pair<Mutator<Object>, ThriftAbstractDao>(mutator,
						columnFamilyDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getCounterMutator()
	{
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(AchillesCounter.THRIFT_COUNTER_CF))
		{
			mutator = mutatorMap.get(AchillesCounter.THRIFT_COUNTER_CF).left;
		}
		else
		{
			ThriftCounterDao thriftCounterDao = thriftDaoContext.getCounterDao();
			mutator = thriftCounterDao.buildMutator();
			mutatorMap.put(AchillesCounter.THRIFT_COUNTER_CF,
					new Pair<Mutator<Object>, ThriftAbstractDao>(mutator, thriftCounterDao));
		}
		return mutator;
	}

	public ThriftConsistencyContext getConsistencyContext()
	{
		return consistencyContext;
	}
}
