package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.type.ConsistencyLevel;

/**
 * ThriftConsistencyContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyContext
{
    private final AchillesConsistencyLevelPolicy policy;

    private ConsistencyLevel consistencyLevel;

    public ThriftConsistencyContext(AchillesConsistencyLevelPolicy policy, ConsistencyLevel consistencyLevel)
    {
        this.policy = policy;
        this.consistencyLevel = consistencyLevel;
    }

    public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context)
    {
        if (consistencyLevel != null)
        {
            policy.setCurrentReadLevel(consistencyLevel);
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

        if (readLevel != null)
        {
            policy.setCurrentReadLevel(readLevel);
            return reinitConsistencyLevels(context);
        }
        else
        {
            return context.execute();
        }
    }

    public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context)
    {
        if (consistencyLevel != null)
        {
            policy.setCurrentWriteLevel(consistencyLevel);
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

        if (writeLevel != null)
        {
            policy.setCurrentWriteLevel(writeLevel);
            return reinitConsistencyLevels(context);
        }
        else
        {
            return context.execute();
        }
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel != null)
        {
            this.consistencyLevel = consistencyLevel;
            policy.setCurrentReadLevel(consistencyLevel);
        }
    }

    public void setConsistencyLevel()
    {
        if (consistencyLevel != null)
        {
            policy.setCurrentReadLevel(consistencyLevel);
        }
    }

    public void reinitConsistencyLevels()
    {
        policy.reinitCurrentConsistencyLevels();
        policy.reinitDefaultConsistencyLevels();
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
    {
        try
        {
            return context.execute();
        } finally
        {
            policy.reinitCurrentConsistencyLevels();
            policy.reinitDefaultConsistencyLevels();
        }
    }

}
