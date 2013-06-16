package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

import com.google.common.base.Optional;

/**
 * CQLImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLImmediateFlushContext extends CQLAbstractFlushContext
{

	public CQLImmediateFlushContext(CQLDaoContext daoContext,
			Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
			Optional<Integer> ttlO)
	{
		super(daoContext, readLevelO, writeLevelO, ttlO);
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

}
