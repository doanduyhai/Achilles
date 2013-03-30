package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.exception.AchillesException;

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
	protected final Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;
	protected final Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap;
	protected final CounterDao counterDao;

	protected final Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>();

	public AbstractBatchContext(Map<String, GenericDynamicCompositeDao<?>> entityDaosMap,
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
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
			GenericDynamicCompositeDao<ID> entityDao = (GenericDynamicCompositeDao<ID>) entityDaosMap
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
			GenericCompositeDao<ID, ?> columnFamilyDao = (GenericCompositeDao<ID, ?>) columnFamilyDaosMap
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
		catch (Throwable throwable)
		{
			throw new AchillesException(throwable);
		}
		finally
		{
			mutatorMap.clear();
		}
	}

	public abstract <ID> void flush();

	public abstract <ID> void endBatch();

	public abstract BatchType type();

	public static enum BatchType
	{
		NONE,
		BATCH
	}
}
