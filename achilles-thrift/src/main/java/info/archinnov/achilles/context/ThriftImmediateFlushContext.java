package info.archinnov.achilles.context;

import static info.archinnov.achilles.context.FlushContext.FlushType.IMMEDIATE;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            AchillesConsistencyLevelPolicy policy, ConsistencyLevel consistencyLevel)
    {
        super(thriftDaoContext, policy, consistencyLevel);
    }

    public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
            ThriftConsistencyContext consistencyContext,
            Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
            boolean hasCustomConsistencyLevels)
    {
        super(thriftDaoContext, consistencyContext, mutatorMap, hasCustomConsistencyLevels);
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
    public ThriftImmediateFlushContext duplicate()
    {
        return new ThriftImmediateFlushContext(thriftDaoContext,
                consistencyContext, mutatorMap, hasCustomConsistencyLevels);
    }
}
