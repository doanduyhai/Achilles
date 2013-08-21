package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

/**
 * FlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class FlushContext<T extends FlushContext<T>>
{

    public abstract void startBatch();

    public abstract void flush();

    public abstract void endBatch();

    public abstract void cleanUp();

    public abstract FlushType type();

    public abstract T duplicate();

    public abstract void setConsistencyLevel(ConsistencyLevel consistencyLevel);

    public abstract ConsistencyLevel getConsistencyLevel();

    public static enum FlushType
    {
        IMMEDIATE,
        BATCH
    }

}
