package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.context.FlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class BatchingFlushContext extends AbstractFlushContext
{

	private static final Logger log = LoggerFactory.getLogger(ImmediateFlushContext.class);

	public BatchingFlushContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao,
			AchillesConfigurableConsistencyLevelPolicy policy)
	{
		super(entityDaosMap, columnFamilyDaosMap, counterDao, policy);
	}

	@Override
	public void startBatch()
	{
		log.debug("Starting a new batch");
		super.cleanUp();
	}

	@Override
	public void flush()
	{
		log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
	}

	@Override
	public void endBatch()
	{
		log.debug("Ending current batch");
		doFlush();
	}

	@Override
	public FlushType type()
	{
		return BATCH;
	}

}
