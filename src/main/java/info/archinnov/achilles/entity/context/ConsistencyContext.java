package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy.currentReadConsistencyLevel;
import static info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy.currentWriteConsistencyLevel;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

/**
 * ConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConsistencyContext
{
	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		Validator.validateNotNull(writeLevel, "Consistency level should not be null");
		currentWriteConsistencyLevel.set(writeLevel);
	}

	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		Validator.validateNotNull(readLevel, "Consistency level should not be null");
		currentReadConsistencyLevel.set(readLevel);
	}

	public void reinitConsistencyLevels()
	{
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
	}
}
