package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
import com.google.common.base.Optional;

/**
 * AchillesFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class FlushContext<T extends FlushContext<T>>
{
    protected Optional<Integer> ttlO;

    public FlushContext(Optional<Integer> ttlO) {
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

    public abstract Optional<ConsistencyLevel> getReadConsistencyLevel();

    public abstract Optional<ConsistencyLevel> getWriteConsistencyLevel();

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
