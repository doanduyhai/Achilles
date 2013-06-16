package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

import com.google.common.base.Optional;

/**
 * ConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public interface ConsistencyContext
{

	public void setWriteConsistencyLevel(Optional<ConsistencyLevel> writeLevel);

	public void setReadConsistencyLevel(Optional<ConsistencyLevel> readLevel);

	public void reinitConsistencyLevels();

}
