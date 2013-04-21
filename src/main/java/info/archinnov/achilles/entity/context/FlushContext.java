package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.entity.type.ConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * FlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public interface FlushContext
{

	public void startBatch();

	public void flush();

	public void endBatch();

	public void cleanUp();

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel);

	public void setReadConsistencyLevel(ConsistencyLevel readLevel);

	public void reinitConsistencyLevels();

	public <ID> Mutator<ID> getEntityMutator(String columnFamilyName);

	public <ID> Mutator<ID> getColumnFamilyMutator(String columnFamilyName);

	public Mutator<Composite> getCounterMutator();

	public FlushType type();

	public static enum FlushType
	{
		IMMEDIATE,
		BATCH
	}

}