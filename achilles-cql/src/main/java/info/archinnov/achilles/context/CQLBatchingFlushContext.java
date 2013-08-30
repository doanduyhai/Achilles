package info.archinnov.achilles.context;

import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLBatchingFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLBatchingFlushContext extends CQLAbstractFlushContext<CQLBatchingFlushContext> {
    private static final Logger log = LoggerFactory.getLogger(CQLBatchingFlushContext.class);

    public CQLBatchingFlushContext(CQLDaoContext daoContext, ConsistencyLevel consistencyLevel)
    {
        super(daoContext, consistencyLevel);
    }

    private CQLBatchingFlushContext(CQLDaoContext daoContext, List<BoundStatementWrapper> boundStatementWrappers,
            ConsistencyLevel consistencyLevel) {
        super(daoContext, boundStatementWrappers, consistencyLevel);
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
    public CQLBatchingFlushContext duplicate() {
        return new CQLBatchingFlushContext(daoContext, boundStatementWrappers, consistencyLevel);
    }

}
