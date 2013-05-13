package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

/**
 * FlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesFlushContext
{
	protected AchillesConsistencyContext consistencyContext;

	public abstract void startBatch();

	public abstract void flush();

	public abstract void endBatch();

	public abstract void cleanUp();

	public abstract void setWriteConsistencyLevel(ConsistencyLevel writeLevel);

	public abstract void setReadConsistencyLevel(ConsistencyLevel readLevel);

	public abstract void reinitConsistencyLevels();

	public abstract FlushType type();

	public static enum FlushType
	{
		IMMEDIATE,
		BATCH
	}

}