package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.type.ConsistencyLevel;

import com.google.common.base.Optional;

/**
 * ThriftConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyContext implements ConsistencyContext
{
	private final AchillesConsistencyLevelPolicy policy;

	private Optional<ConsistencyLevel> readLevelO;
	private Optional<ConsistencyLevel> writeLevelO;

	public ThriftConsistencyContext(AchillesConsistencyLevelPolicy policy,
			Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO)
	{
		this.policy = policy;
		this.readLevelO = readLevelO;
		this.writeLevelO = writeLevelO;
	}

	public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context)
	{
		if (readLevelO.isPresent())
		{
			policy.setCurrentReadLevel(readLevelO.get());
			return reinitConsistencyLevels(context);
		}
		else
		{
			return context.execute();
		}
	}

	public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context,
			ConsistencyLevel readLevel)
	{
		Optional<ConsistencyLevel> levelO = readLevelO.isPresent() ? readLevelO : Optional
				.fromNullable(readLevel);

		if (levelO.isPresent())
		{
			policy.setCurrentReadLevel(levelO.get());
			return reinitConsistencyLevels(context);
		}
		else
		{
			return context.execute();
		}
	}

	public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context)
	{
		if (writeLevelO.isPresent())
		{
			policy.setCurrentWriteLevel(writeLevelO.get());
			return reinitConsistencyLevels(context);
		}
		else
		{
			return context.execute();
		}
	}

	public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context,
			ConsistencyLevel writeLevel)
	{
		Optional<ConsistencyLevel> levelO = writeLevelO.isPresent() ? writeLevelO : Optional
				.fromNullable(writeLevel);

		if (levelO.isPresent())
		{
			policy.setCurrentWriteLevel(levelO.get());
			return reinitConsistencyLevels(context);
		}
		else
		{
			return context.execute();
		}
	}

	@Override
	public void setReadConsistencyLevel(Optional<ConsistencyLevel> readLevelO)
	{
		if (readLevelO.isPresent())
		{
			this.readLevelO = readLevelO;
			policy.setCurrentReadLevel(readLevelO.get());
		}
	}

	public void setReadConsistencyLevel()
	{
		if (readLevelO.isPresent())
		{
			policy.setCurrentReadLevel(readLevelO.get());
		}
	}

	@Override
	public void setWriteConsistencyLevel(Optional<ConsistencyLevel> writeLevelO)
	{
		if (writeLevelO.isPresent())
		{
			this.writeLevelO = writeLevelO;
			policy.setCurrentWriteLevel(writeLevelO.get());
		}
	}

	public void setWriteConsistencyLevel()
	{
		if (writeLevelO.isPresent())
		{
			policy.setCurrentWriteLevel(writeLevelO.get());
		}
	}

	@Override
	public void reinitConsistencyLevels()
	{
		policy.reinitCurrentConsistencyLevels();
		policy.reinitDefaultConsistencyLevels();
	}

	public Optional<ConsistencyLevel> getReadLevelO()
	{
		return readLevelO;
	}

	public Optional<ConsistencyLevel> getWriteLevelO()
	{
		return writeLevelO;
	}

	private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
	{
		try
		{
			return context.execute();
		}
		finally
		{
			policy.reinitCurrentConsistencyLevels();
			policy.reinitDefaultConsistencyLevels();
		}
	}

}
