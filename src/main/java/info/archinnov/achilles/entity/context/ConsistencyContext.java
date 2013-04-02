package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
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
	private final AchillesConfigurableConsistencyLevelPolicy policy;

	public ConsistencyContext(AchillesConfigurableConsistencyLevelPolicy policy) {
		this.policy = policy;
	}

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		Validator.validateNotNull(writeLevel, "Consistency level should not be null");
		policy.setCurrentWriteLevel(writeLevel);
	}

	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		Validator.validateNotNull(readLevel, "Consistency level should not be null");
		policy.setCurrentReadLevel(readLevel);
	}

	public void reinitConsistencyLevels()
	{
		policy.reinitCurrentConsistencyLevels();
		policy.reinitDefaultConsistencyLevels();
	}
}
