package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

/**
 * ThriftConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyContext implements ConsistencyContext
{
	private final AchillesConsistencyLevelPolicy policy;

	public ThriftConsistencyContext(AchillesConsistencyLevelPolicy policy) {
		this.policy = policy;
	}

	@Override
	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		Validator.validateNotNull(writeLevel, "Consistency level should not be null");
		policy.setCurrentWriteLevel(writeLevel);
	}

	@Override
	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		Validator.validateNotNull(readLevel, "Consistency level should not be null");
		policy.setCurrentReadLevel(readLevel);
	}

	@Override
	public void reinitConsistencyLevels()
	{
		policy.reinitCurrentConsistencyLevels();
		policy.reinitDefaultConsistencyLevels();
	}
}
