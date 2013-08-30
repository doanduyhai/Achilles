package info.archinnov.achilles.context;

import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLImmediateFlushContext extends CQLAbstractFlushContext<CQLImmediateFlushContext>
{
    private static final Logger log = LoggerFactory.getLogger(CQLImmediateFlushContext.class);

    public CQLImmediateFlushContext(CQLDaoContext daoContext, ConsistencyLevel consistencyLevel)
    {
        super(daoContext, consistencyLevel);
    }

    private CQLImmediateFlushContext(CQLDaoContext daoContext,
            List<BoundStatementWrapper> boundStatementWrappers, ConsistencyLevel consistencyLevel)
    {
        super(daoContext, boundStatementWrappers, consistencyLevel);
    }

    @Override
    public void startBatch()
    {
        throw new UnsupportedOperationException(
                "Cannot start a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
    }

    @Override
    public void endBatch()
    {
        throw new UnsupportedOperationException(
                "Cannot end a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
    }

    @Override
    public void flush()
    {
        log.debug("Flush immediatly all pending statements");
        doFlush();
    }

    @Override
    public FlushType type()
    {
        return FlushType.IMMEDIATE;
    }

    @Override
    public CQLImmediateFlushContext duplicate()
    {
        return new CQLImmediateFlushContext(daoContext,
                boundStatementWrappers, consistencyLevel);
    }
}
