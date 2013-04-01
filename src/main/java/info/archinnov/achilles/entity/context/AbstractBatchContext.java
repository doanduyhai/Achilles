package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
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

/**
 * BatchContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractBatchContext
{
	protected final Map<String, GenericEntityDao<?>> entityDaosMap;
	protected final Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	protected final CounterDao counterDao;

	protected final Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>();
	protected ConsistencyContext consistencyContext = new ConsistencyContext();
	protected boolean hasCustomConsistencyLevels = false;

	public AbstractBatchContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
	{
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
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
			GenericEntityDao<ID> entityDao = (GenericEntityDao<ID>) entityDaosMap
					.get(columnFamilyName);

			if (entityDao != null)
			{
				mutator = entityDao.buildMutator();
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, AbstractDao<?, ?, ?>>(
						mutator, entityDao));
			}
		}
		return mutator;
	}

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
				mutatorMap.put(columnFamilyName, new Pair<Mutator<?>, AbstractDao<?, ?, ?>>(
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
			mutator = counterDao.buildMutator();
			mutatorMap.put(COUNTER_CF, new Pair<Mutator<?>, AbstractDao<?, ?, ?>>(mutator,
					counterDao));
		}
		return mutator;
	}

	@SuppressWarnings("unchecked")
	protected <ID> void doFlush()
	{
		try
		{
			for (Entry<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> entry : mutatorMap
					.entrySet())
			{
				AbstractDao<ID, ?, ?> dao = (AbstractDao<ID, ?, ?>) entry.getValue().right;
				Mutator<ID> mutator = (Mutator<ID>) entry.getValue().left;
				dao.executeMutator(mutator);
			}
		}
		finally
		{
			mutatorMap.clear();
		}
	}

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setWriteConsistencyLevel(writeLevel);
	}

	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		hasCustomConsistencyLevels = true;
		consistencyContext.setReadConsistencyLevel(readLevel);
	}

	public abstract <ID> void flush();

	public abstract <ID> void endBatch();

	public abstract BatchType type();

	public abstract void reinitConsistencyLevels();

	public static enum BatchType
	{
		NONE,
		BATCH
	}
}
