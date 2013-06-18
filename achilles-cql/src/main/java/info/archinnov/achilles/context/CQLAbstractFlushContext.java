package info.archinnov.achilles.context;

import static info.archinnov.achilles.consistency.CQLConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
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
	public void startBatch()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void endBatch()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanUp()
	{
		// TODO Auto-generated method stub

	}

	protected void doFlush()
	{
		for (BoundStatement bs : boundStatements)
		{
			daoContext.execute(bs);
		}
		boundStatements.clear();

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
		// TODO Auto-generated method stub

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

	public ResultSet executeImmediateWithConsistency(Query query,
			ConsistencyLevel readConsistencyLevel)
	{
		if (readLevelO.isPresent())
		{
			query.setConsistencyLevel(getCQLLevel(readLevelO.get()));
		}
		else
		{
			query.setConsistencyLevel(getCQLLevel(readConsistencyLevel));
		}

		return daoContext.execute(query);
	}

	public List<BoundStatement> getBoundStatements()
	{
		return boundStatements;
	}

}
