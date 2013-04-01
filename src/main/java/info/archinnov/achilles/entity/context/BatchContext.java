package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

/**
 * BatchContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class BatchContext extends AbstractBatchContext
{

	/**
	 * @param entityDaosMap
	 * @param columnFamilyDaosMap
	 * @param counterDao
	 */
	public BatchContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
	{
		super(entityDaosMap, columnFamilyDaosMap, counterDao);
	}

	@Override
	public <ID> void flush()
	{
		// Do nothing. Only flush when endBatch() is called
	}

	@Override
	public <ID> void endBatch()
	{
		doFlush();
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
	public BatchType type()
	{
		return BatchType.BATCH;
	}

}
