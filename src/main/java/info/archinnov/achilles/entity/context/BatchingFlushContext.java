package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.context.FlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;

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

	public BatchingFlushContext(DaoContext daoContext,
			AchillesConfigurableConsistencyLevelPolicy policy)
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
