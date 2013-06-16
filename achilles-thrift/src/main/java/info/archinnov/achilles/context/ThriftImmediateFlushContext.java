package info.archinnov.achilles.context;

import static info.archinnov.achilles.context.AchillesFlushContext.FlushType.IMMEDIATE;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.type.ConsistencyLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * ThriftImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftImmediateFlushContext extends ThriftAbstractFlushContext
{
	private static final Logger log = LoggerFactory.getLogger(ThriftImmediateFlushContext.class);

	public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy, Optional<ConsistencyLevel> readLevelO,
			Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		super(thriftDaoContext, policy, readLevelO, writeLevelO, ttlO);
	}

	@Override
	public void startBatch()
	{
		throw new UnsupportedOperationException(
				"Cannot start a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public void flush()
	{
		log.debug("Flush immediatly all pending mutations");
		doFlush();
	}

	@Override
	public void endBatch()
	{
		throw new UnsupportedOperationException(
				"Cannot end a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public FlushType type()
	{
		return IMMEDIATE;
	}
}
