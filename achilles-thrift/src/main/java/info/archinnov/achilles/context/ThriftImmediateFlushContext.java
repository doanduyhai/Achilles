package info.archinnov.achilles.context;

import static info.archinnov.achilles.context.FlushContext.FlushType.IMMEDIATE;
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
 * ThriftImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftImmediateFlushContext extends
		ThriftAbstractFlushContext<ThriftImmediateFlushContext>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftImmediateFlushContext.class);

	public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy, Optional<ConsistencyLevel> readLevelO,
			Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		super(thriftDaoContext, policy, readLevelO, writeLevelO, ttlO);
	}

	public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
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

	@Override
	public ThriftImmediateFlushContext duplicateWithoutTtl()
	{
		return new ThriftImmediateFlushContext(thriftDaoContext,
				consistencyContext,
				mutatorMap,
				hasCustomConsistencyLevels,
				Optional.<Integer> absent());
	}
}
