package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

import com.google.common.base.Optional;

/**
 * AchillesFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesFlushContext<T extends AchillesFlushContext<T>>
{
	protected Optional<Integer> ttlO;

	public AchillesFlushContext(Optional<Integer> ttlO) {
		this.ttlO = ttlO;
	}

	public Optional<Integer> getTtlO()
	{
		return ttlO;
	}

	public abstract void startBatch();

	public abstract void flush();

	public abstract void endBatch();

	public abstract void cleanUp();

	public abstract void setWriteConsistencyLevel(Optional<ConsistencyLevel> writeLevelO);

	public abstract void setReadConsistencyLevel(Optional<ConsistencyLevel> readLevelO);

	public abstract void reinitConsistencyLevels();

	public abstract FlushType type();

	public abstract T duplicateWithoutTtl();

	public static enum FlushType
	{
		IMMEDIATE,
		BATCH
	}

}