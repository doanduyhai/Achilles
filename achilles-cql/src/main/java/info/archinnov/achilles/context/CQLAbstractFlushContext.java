package info.archinnov.achilles.context;

import static info.archinnov.achilles.helper.CQLConsistencyHelper.getCQLLevel;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Statement;

/**
 * CQLFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class CQLAbstractFlushContext extends AchillesFlushContext
{

	private List<BoundStatement> boundStatements = new ArrayList<BoundStatement>();
	private List<Statement> statements = new ArrayList<Statement>();
	private ConsistencyLevel readevel;
	private ConsistencyLevel writeLevel;

	@Override
	public void startBatch()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void flush()
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

	@Override
	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
	}

	@Override
	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		this.readevel = readLevel;
	}

	@Override
	public void reinitConsistencyLevels()
	{
		// TODO Auto-generated method stub

	}

	public void pushBoundStatement(BoundStatement boundStatement, EntityMeta entityMeta)
	{
		if (writeLevel != null)
		{
			boundStatement.setConsistencyLevel(getCQLLevel(writeLevel));
		}
		else
		{
			boundStatement.setConsistencyLevel(getCQLLevel(entityMeta.getWriteConsistencyLevel()));
		}
		boundStatements.add(boundStatement);
	}

	public void pushBoundStatement(BoundStatement boundStatement, PropertyMeta<?, ?> propertyMeta)
	{
		if (writeLevel != null)
		{
			boundStatement.setConsistencyLevel(getCQLLevel(writeLevel));
		}
		else
		{
			boundStatement
					.setConsistencyLevel(getCQLLevel(propertyMeta.getWriteConsistencyLevel()));
		}
		boundStatements.add(boundStatement);
	}

	public List<BoundStatement> getBoundStatements()
	{
		return boundStatements;
	}

	public List<Statement> getStatements()
	{
		return statements;
	}

	public void addStatement(Statement statement)
	{
		statements.add(statement);
	}
}
