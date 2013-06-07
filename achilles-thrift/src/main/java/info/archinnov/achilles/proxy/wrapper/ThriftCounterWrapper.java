package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.ThriftSafeExecutionContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCounterWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterWrapper implements Counter
{
	private static final Logger log = LoggerFactory.getLogger(ThriftCounterWrapper.class);

	private static final long serialVersionUID = 1L;
	private Object key;
	private Composite columnName;
	private ThriftAbstractDao counterDao;
	private ThriftPersistenceContext context;
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;

	private EntityValidator validator = new EntityValidator(
			new ThriftEntityProxifier());

	public ThriftCounterWrapper(ThriftPersistenceContext context) {
		this.context = context;
	}

	@Override
	public Long get()
	{
		return executeWithReadConsistencyLevel(new ThriftSafeExecutionContext<Long>()
		{
			@Override
			public Long execute()
			{
				return counterDao.getCounterValue(key, columnName);
			}
		}, readLevel);
	}

	@Override
	public Long get(ConsistencyLevel readLevel)
	{
		validator.validateNoPendingBatch(context);
		return executeWithReadConsistencyLevel(new ThriftSafeExecutionContext<Long>()
		{
			@Override
			public Long execute()
			{
				return counterDao.getCounterValue(key, columnName);
			}
		}, readLevel);
	}

	@Override
	public void incr()
	{
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);

	}

	public void incr(ConsistencyLevel writeLevel)
	{
		validator.validateNoPendingBatch(context);
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);

	}

	@Override
	public void incr(final Long increment)
	{
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, increment);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void incr(final Long increment, ConsistencyLevel writeLevel)
	{
		validator.validateNoPendingBatch(context);
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.incrementCounter(key, columnName, increment);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void decr()
	{
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);

	}

	@Override
	public void decr(ConsistencyLevel writeLevel)
	{
		validator.validateNoPendingBatch(context);
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void decr(final Long decrement)
	{
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void decr(final Long decrement, ConsistencyLevel writeLevel)
	{
		validator.validateNoPendingBatch(context);
		executeWithWriteConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		}, writeLevel);
	}

	private <T> T executeWithWriteConsistencyLevel(ThriftSafeExecutionContext<T> context,
			final ConsistencyLevel writeLevel)
	{
		log.trace("Execute write with runtime consistency level {}", writeLevel.name());

		boolean resetConsistencyLevel = false;
		ThriftConsistencyLevelPolicy policy = (ThriftConsistencyLevelPolicy) this.context
				.getPolicy();
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

	private <T> T executeWithReadConsistencyLevel(ThriftSafeExecutionContext<T> context,
			final ConsistencyLevel readLevel)
	{
		log.trace("Execute read with runtime consistency level {}", readLevel.name());
		boolean resetConsistencyLevel = false;
		ThriftConsistencyLevelPolicy policy = (ThriftConsistencyLevelPolicy) this.context
				.getPolicy();
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

	public void setCounterDao(ThriftAbstractDao counterDao)
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

	public void setKey(Object key)
	{
		this.key = key;
	}
}
