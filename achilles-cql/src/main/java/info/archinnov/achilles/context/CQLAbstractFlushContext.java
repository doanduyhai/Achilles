package info.archinnov.achilles.context;

import static info.archinnov.achilles.consistency.CQLConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;

/**
 * CQLFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class CQLAbstractFlushContext<T extends CQLAbstractFlushContext<T>> extends
        FlushContext<T>
{

    protected Optional<ConsistencyLevel> readLevelO;
    protected Optional<ConsistencyLevel> writeLevelO;
    protected CQLDaoContext daoContext;

    protected List<BoundStatement> boundStatements = new ArrayList<BoundStatement>();
    protected List<Statement> statements = new ArrayList<Statement>();

    public CQLAbstractFlushContext(CQLDaoContext daoContext, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        super(ttlO);
        this.daoContext = daoContext;
        this.readLevelO = readLevelO;
        this.writeLevelO = writeLevelO;
    }

    protected CQLAbstractFlushContext(CQLDaoContext daoContext,
            List<BoundStatement> boundStatements,
            Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        super(ttlO);
        this.boundStatements = boundStatements;
        this.daoContext = daoContext;
        this.readLevelO = readLevelO;
        this.writeLevelO = writeLevelO;
    }

    @Override
    public void cleanUp()
    {
        reinitConsistencyLevels();
        boundStatements.clear();
        statements.clear();
        ttlO = Optional.<Integer> absent();
    }

    protected void doFlush()
    {
        for (BoundStatement bs : boundStatements)
        {
            daoContext.execute(bs);
        }
        for (Statement statement : statements)
        {
            daoContext.execute(statement);
        }

        cleanUp();

    }

    @Override
    public void setWriteConsistencyLevel(Optional<ConsistencyLevel> writeLevelO)
    {
        this.writeLevelO = writeLevelO;
    }

    @Override
    public void setReadConsistencyLevel(Optional<ConsistencyLevel> readLevelO)
    {
        this.readLevelO = readLevelO;
    }

    @Override
    public void reinitConsistencyLevels()
    {
        readLevelO = Optional.<ConsistencyLevel> absent();
        writeLevelO = Optional.<ConsistencyLevel> absent();

    }

    public void pushBoundStatement(BoundStatement boundStatement,
            ConsistencyLevel writeConsistencyLevel)
    {
        if (writeLevelO.isPresent())
        {
            boundStatement.setConsistencyLevel(getCQLLevel(writeLevelO.get()));
        }
        else
        {
            boundStatement.setConsistencyLevel(getCQLLevel(writeConsistencyLevel));
        }
        boundStatements.add(boundStatement);
    }

    public void pushStatement(Statement statement,
            ConsistencyLevel writeConsistencyLevel)
    {
        if (writeLevelO.isPresent())
        {
            statement.setConsistencyLevel(getCQLLevel(writeLevelO.get()));
        }
        else
        {
            statement.setConsistencyLevel(getCQLLevel(writeConsistencyLevel));
        }
        statements.add(statement);
    }

    public ResultSet executeImmediateWithConsistency(Query query,
            ConsistencyLevel readConsistencyLevel)
    {
        query.setConsistencyLevel(getCQLLevel(readConsistencyLevel));
        return daoContext.execute(query);
    }

    public List<BoundStatement> getBoundStatements()
    {
        return boundStatements;
    }

    @Override
    public Optional<ConsistencyLevel> getReadConsistencyLevel() {
        return readLevelO;
    }

    @Override
    public Optional<ConsistencyLevel> getWriteConsistencyLevel() {
        return writeLevelO;
    }

}
