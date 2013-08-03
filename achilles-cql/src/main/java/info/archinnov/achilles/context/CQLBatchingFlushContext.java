package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Optional;

/**
 * CQLBatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLBatchingFlushContext extends CQLAbstractFlushContext<CQLBatchingFlushContext> {
    private static final Logger log = LoggerFactory.getLogger(CQLBatchingFlushContext.class);

    public CQLBatchingFlushContext(CQLDaoContext daoContext,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        super(daoContext, readLevelO, writeLevelO, ttlO);
    }

    private CQLBatchingFlushContext(CQLDaoContext daoContext, List<BoundStatement> boundStatements,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO) {
        super(daoContext, boundStatements, readLevelO, writeLevelO, ttlO);
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
        doFlush();
    }

    @Override
    public FlushType type() {
        return FlushType.BATCH;
    }

    @Override
    public CQLBatchingFlushContext duplicateWithoutTtl() {
        return new CQLBatchingFlushContext(daoContext, boundStatements, readLevelO, writeLevelO,
                Optional.<Integer> absent());
    }

}
