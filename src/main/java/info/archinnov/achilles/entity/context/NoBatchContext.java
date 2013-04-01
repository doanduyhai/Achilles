package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

/**
 * NoBatchContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class NoBatchContext extends AbstractBatchContext
{

	/**
	 * @param entityDaosMap
	 * @param columnFamilyDaosMap
	 * @param counterDao
	 */
	public NoBatchContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
	{
		super(entityDaosMap, columnFamilyDaosMap, counterDao);
	}

	@Override
	public <ID> void flush()
	{
		doFlush();
	}

	@Override
	public <ID> void endBatch()
	{
		throw new UnsupportedOperationException(
				"The method 'endBatch()' is not supported for a NoBatchContext. Please use it within a batch context");

	}

	@Override
	public BatchType type()
	{
		return BatchType.NONE;
	}

	@Override
	public void reinitConsistencyLevels()
	{
		consistencyContext.reinitConsistencyLevels();
	}
}
