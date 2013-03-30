package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;

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
	public BatchContext(Map<String, GenericDynamicCompositeDao<?>> entityDaosMap,
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
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
	public BatchType type()
	{
		return BatchType.BATCH;
	}

}
