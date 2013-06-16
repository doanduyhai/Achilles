package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Optional;

/**
 * CQLImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLImmediateFlushContext extends CQLAbstractFlushContext<CQLImmediateFlushContext>
{

	public CQLImmediateFlushContext(CQLDaoContext daoContext,
			Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
			Optional<Integer> ttlO)
	{
		super(daoContext, readLevelO, writeLevelO, ttlO);
	}

	private CQLImmediateFlushContext(CQLDaoContext daoContext,
			List<BoundStatement> boundStatements,
			Optional<ConsistencyLevel> readLevelO,
			Optional<ConsistencyLevel> writeLevelO,
			Optional<Integer> ttlO)
	{
		super(daoContext, boundStatements, readLevelO, writeLevelO, ttlO);
	}

	@Override
	public void flush()
	{
		super.doFlush();
	}

	@Override
	public FlushType type()
	{
		return FlushType.IMMEDIATE;
	}

	@Override
	public CQLImmediateFlushContext duplicateWithoutTtl()
	{
		return new CQLImmediateFlushContext(daoContext,
				boundStatements,
				readLevelO,
				writeLevelO,
				Optional.<Integer> absent());
	}

}
