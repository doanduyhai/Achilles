package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

/**
 * CQLCounterWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLCounterWrapper implements Counter {

    private CQLPersistenceContext context;
    private PropertyMeta counterMeta;
    private boolean clusteredCounter;

    public CQLCounterWrapper(CQLPersistenceContext context, PropertyMeta counterMeta) {
        this.context = context;
        this.counterMeta = counterMeta;
        this.clusteredCounter = context.getEntityMeta().isClusteredCounter();
    }

    @Override
    public Long get() {
        ConsistencyLevel readLevel = getReadRuntimeConsistencyIfPossible();
        if (clusteredCounter)
            return context.getClusteredCounter(counterMeta, readLevel);
        else
            return context.getSimpleCounter(counterMeta, readLevel);
    }

    @Override
    public Long get(ConsistencyLevel readLevel) {
        if (clusteredCounter)
            return context.getClusteredCounter(counterMeta, readLevel);
        else
            return context.getSimpleCounter(counterMeta, readLevel);
    }

    @Override
    public void incr() {
        ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
        if (clusteredCounter)
            context.incrementClusteredCounter(counterMeta, 1L, writeLevel);
        else
            context.incrementSimpleCounter(counterMeta, 1L, writeLevel);
    }

    @Override
    public void incr(ConsistencyLevel writeLevel) {
        if (clusteredCounter)
            context.incrementClusteredCounter(counterMeta, 1L, writeLevel);
        else
            context.incrementSimpleCounter(counterMeta, 1L, writeLevel);
    }

    @Override
    public void incr(Long increment) {
        ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
        if (clusteredCounter)
            context.incrementClusteredCounter(counterMeta, increment, writeLevel);
        else
            context.incrementSimpleCounter(counterMeta, increment, writeLevel);
    }

    @Override
    public void incr(Long increment, ConsistencyLevel writeLevel) {
        if (clusteredCounter)
            context.incrementClusteredCounter(counterMeta, increment, writeLevel);
        else
            context.incrementSimpleCounter(counterMeta, increment, writeLevel);
    }

    @Override
    public void decr() {
        ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
        if (clusteredCounter)
            context.decrementClusteredCounter(counterMeta, 1L, writeLevel);
        else
            context.decrementSimpleCounter(counterMeta, 1L, writeLevel);
    }

    @Override
    public void decr(ConsistencyLevel writeLevel) {
        if (clusteredCounter)
            context.decrementClusteredCounter(counterMeta, 1L, writeLevel);
        else
            context.decrementSimpleCounter(counterMeta, 1L, writeLevel);
    }

    @Override
    public void decr(Long decrement) {
        ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
        if (clusteredCounter)
            context.decrementClusteredCounter(counterMeta, decrement, writeLevel);
        else
            context.decrementSimpleCounter(counterMeta, decrement, writeLevel);
    }

    @Override
    public void decr(Long decrement, ConsistencyLevel writeLevel) {
        if (clusteredCounter)
            context.decrementClusteredCounter(counterMeta, decrement, writeLevel);
        else
            context.decrementSimpleCounter(counterMeta, decrement, writeLevel);
    }

    private ConsistencyLevel getReadRuntimeConsistencyIfPossible()
    {
        return context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : counterMeta
                .getReadConsistencyLevel();
    }

    private ConsistencyLevel getWriteRuntimeConsistencyIfPossible()
    {
        return context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : counterMeta
                .getWriteConsistencyLevel();
    }
}
