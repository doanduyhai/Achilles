package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;

/**
 * ConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public interface ConsistencyContext {

    public void setWriteConsistencyLevel(ConsistencyLevel writeLevel);

    public void setReadConsistencyLevel(ConsistencyLevel readLevel);

    public void reinitConsistencyLevels();

}
