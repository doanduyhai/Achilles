package info.archinnov.achilles.context;

import static info.archinnov.achilles.consistency.CQLConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;

/**
 * CQLFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class CQLAbstractFlushContext extends AchillesFlushContext
{

	private List<BoundStatement> boundStatements = new ArrayList<BoundStatement>();
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;
	private CQLDaoContext daoContext;

	public CQLAbstractFlushContext(CQLDaoContext daoContext) {
		this.daoContext = daoContext;
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
	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
	}

	@Override
	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		this.readLevel = readLevel;
	}

	@Override
	public void reinitConsistencyLevels()
	{
		// TODO Auto-generated method stub

	}

	public void pushBoundStatement(BoundStatement boundStatement, ConsistencyLevel writeLevel)
	{
		if (this.writeLevel != null)
		{
			boundStatement.setConsistencyLevel(getCQLLevel(this.writeLevel));

		}
		else
		{
			boundStatement.setConsistencyLevel(getCQLLevel(writeLevel));
		}
		boundStatements.add(boundStatement);
	}

	public ResultSet executeImmediateWithConsistency(Query query, EntityMeta entityMeta)
	{
		if (readLevel != null)
		{
			query.setConsistencyLevel(getCQLLevel(readLevel));
		}
		else
		{
			query.setConsistencyLevel(getCQLLevel(entityMeta.getReadConsistencyLevel()));
		}

		return daoContext.execute(query);
	}

	public List<BoundStatement> getBoundStatements()
	{
		return boundStatements;
	}

}
