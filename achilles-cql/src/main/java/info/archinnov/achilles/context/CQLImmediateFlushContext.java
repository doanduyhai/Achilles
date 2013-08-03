package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger log = LoggerFactory.getLogger(CQLImmediateFlushContext.class);

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
    public CQLImmediateFlushContext duplicateWithoutTtl()
    {
        return new CQLImmediateFlushContext(daoContext,
                boundStatements,
                readLevelO,
                writeLevelO,
                Optional.<Integer> absent());
    }

}
