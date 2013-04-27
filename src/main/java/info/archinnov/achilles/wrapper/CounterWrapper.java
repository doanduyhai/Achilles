package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.entity.context.execution.SafeExecutionContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
import me.prettyprint.hector.api.beans.Composite;

/**
 * CounterWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWrapper<ID> implements Counter
{
	private static final long serialVersionUID = 1L;
	private ID key;
	private Composite columnName;
	private AbstractDao<ID, Long> counterDao;
	private AchillesConfigurableConsistencyLevelPolicy policy;
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;

	public CounterWrapper(ID key) {
		this.key = key;
	}

	@Override
	public Long get()
	{
		return executeWithReadConsistencyLevel(new SafeExecutionContext<Long>()
		{
			@Override
			public Long execute()
			{
				return counterDao.getCounterValue(key, columnName);
			}
		});
	}

	@Override
	public void incr()
	{
		executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, 1L);
				return null;
			}
		});

	}

	@Override
	public void incr(final Long increment)
	{
		executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, increment);
				return null;
			}
		});
	}

	@Override
	public void decr()
	{
		executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, 1L);
				return null;
			}
		});

	}

	@Override
	public void decr(final Long decrement)
	{
		executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		});
	}

	private <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context)
	{
		boolean resetConsistencyLevel = false;
		if (policy.getCurrentWriteLevel() == null)
		{
			policy.setCurrentWriteLevel(writeLevel);
			resetConsistencyLevel = true;
		}
		try
		{
			return context.execute();
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				policy.removeCurrentWriteLevel();
			}
		}
	}

	private <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context)
	{
		boolean resetConsistencyLevel = false;
		if (policy.getCurrentReadLevel() == null)
		{
			policy.setCurrentReadLevel(readLevel);
			resetConsistencyLevel = true;
		}
		try
		{
			return context.execute();
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				policy.removeCurrentReadLevel();
			}
		}
	}

	public void setCounterDao(AbstractDao<ID, Long> counterDao)
	{
		this.counterDao = counterDao;
	}

	public void setColumnName(Composite columnName)
	{
		this.columnName = columnName;
	}

	public void setReadLevel(ConsistencyLevel readLevel)
	{
		this.readLevel = readLevel;
	}

	public void setWriteLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
	}

	public void setPolicy(AchillesConfigurableConsistencyLevelPolicy policy)
	{
		this.policy = policy;
	}
}
