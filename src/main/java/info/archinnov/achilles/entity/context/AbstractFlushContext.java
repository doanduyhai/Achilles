package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
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
public abstract class AbstractFlushContext implements FlushContext
{
	protected static final Logger log = LoggerFactory.getLogger(AbstractFlushContext.class);

	protected Map<String, GenericEntityDao<?>> entityDaosMap;
	protected Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	protected CounterDao counterDao;

	protected Map<String, Pair<Mutator<?>, AbstractDao<?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?>>>();
	protected ConsistencyContext consistencyContext;
	protected boolean hasCustomConsistencyLevels = false;

	protected AbstractFlushContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao,
			AchillesConfigurableConsistencyLevelPolicy policy)
	{
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.consistencyContext = new ConsistencyContext(policy);
	}

	@SuppressWarnings("unchecked")
	protected <ID> void doFlush()
	{
		log.debug("Execute mutations flush");
		try
		{
			for (Entry<String, Pair<Mutator<?>, AbstractDao<?, ?>>> entry : mutatorMap.entrySet())
			{
				AbstractDao<ID, ?> dao = (AbstractDao<ID, ?>) entry.getValue().right;
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

	@Override
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
			GenericEntityDao<ID> entityDao = (GenericEntityDao<ID>) entityDaosMap
					.get(columnFamilyName);

			if (entityDao != null)
			{
				mutator = entityDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, AbstractDao<?, ?>>(mutator,
						entityDao));
			}
		}
		return mutator;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <ID> Mutator<ID> getColumnFamilyMutator(String columnFamilyName)
	{
		Mutator<ID> mutator = null;
		if (mutatorMap.containsKey(columnFamilyName))
		{
			mutator = (Mutator<ID>) mutatorMap.get(columnFamilyName).left;
		}
		else
		{
			GenericColumnFamilyDao<ID, ?> columnFamilyDao = (GenericColumnFamilyDao<ID, ?>) columnFamilyDaosMap
					.get(columnFamilyName);

			if (columnFamilyDao != null)
			{
				mutator = columnFamilyDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, AbstractDao<?, ?>>(mutator,
						columnFamilyDao));
			}
		}
		return mutator;
	}

	@Override
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
			mutator = counterDao.buildMutator();
			mutatorMap
					.put(COUNTER_CF, new Pair<Mutator<?>, AbstractDao<?, ?>>(mutator, counterDao));
		}
		return mutator;
	}

}
