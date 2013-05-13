package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.context.AchillesFlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftBatchingFlushContext extends ThriftAbstractFlushContext
{

	private static final Logger log = LoggerFactory.getLogger(ThriftImmediateFlushContext.class);

	public ThriftBatchingFlushContext(DaoContext daoContext, AchillesConsistencyLevelPolicy policy)
	{
		super(daoContext, policy);
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
