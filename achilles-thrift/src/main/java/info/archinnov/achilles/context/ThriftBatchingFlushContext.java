package info.archinnov.achilles.context;

import static info.archinnov.achilles.context.FlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;

import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * ThriftBatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftBatchingFlushContext extends
		ThriftAbstractFlushContext<ThriftBatchingFlushContext>
{

	private static final Logger log = LoggerFactory.getLogger(ThriftImmediateFlushContext.class);

	public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy, Optional<ConsistencyLevel> readLevelO,
			Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		super(thriftDaoContext, policy, readLevelO, writeLevelO, ttlO);
	}

	public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
			ThriftConsistencyContext consistencyContext,
			Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
			boolean hasCustomConsistencyLevels,
			Optional<Integer> ttlO)
	{
		super(thriftDaoContext, consistencyContext, mutatorMap, hasCustomConsistencyLevels, ttlO);
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
		consistencyContext.setReadConsistencyLevel();
		consistencyContext.setWriteConsistencyLevel();
		doFlush();
	}

	@Override
	public FlushType type()
	{
		return BATCH;

	}

	@Override
	public ThriftBatchingFlushContext duplicateWithoutTtl()
	{
		return new ThriftBatchingFlushContext(thriftDaoContext,
				consistencyContext,
				mutatorMap,
				hasCustomConsistencyLevels,
				Optional.<Integer> absent());
	}

}
