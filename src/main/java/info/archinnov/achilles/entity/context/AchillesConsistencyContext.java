package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

/**
 * AchillesConsistencyContext
 *
 * @author DuyHai DOAN
 *
 */
public interface AchillesConsistencyContext
{

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel);

	public void setReadConsistencyLevel(ConsistencyLevel readLevel);

	public void reinitConsistencyLevels();

}