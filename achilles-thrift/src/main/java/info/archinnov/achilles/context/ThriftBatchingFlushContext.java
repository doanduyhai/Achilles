package info.archinnov.achilles.context;

import static info.archinnov.achilles.context.FlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftBatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftBatchingFlushContext extends
        ThriftAbstractFlushContext<ThriftBatchingFlushContext>
{

    private static final Logger log = LoggerFactory.getLogger(ThriftImmediateFlushContext.class);

    public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
            AchillesConsistencyLevelPolicy policy, ConsistencyLevel consistencyLevel)
    {
        super(thriftDaoContext, policy, consistencyLevel);
    }

    public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
            ThriftConsistencyContext consistencyContext,
            Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
            boolean hasCustomConsistencyLevels)
    {
        super(thriftDaoContext, consistencyContext, mutatorMap, hasCustomConsistencyLevels);
    }

    @Override
    public void startBatch()
    {
        log.debug("Starting a new batch");
        super.cleanUp();
    }

    @Override
    public void flush()
    {
        log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
    }

    @Override
    public void endBatch()
    {
        log.debug("Ending current batch");
        consistencyContext.setConsistencyLevel();
        doFlush();
    }

    @Override
    public FlushType type()
    {
        return BATCH;

    }

    @Override
    public ThriftBatchingFlushContext duplicate()
    {
        return new ThriftBatchingFlushContext(thriftDaoContext,
                consistencyContext, mutatorMap, hasCustomConsistencyLevels);
    }
}
