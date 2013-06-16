package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
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

	private EntityValidator<ThriftPersistenceContext> validator = new EntityValidator<ThriftPersistenceContext>(
			new ThriftEntityProxifier());

	public ThriftCounterWrapper(ThriftPersistenceContext context) {
		this.context = context;
	}

	@Override
	public Long get()
	{
		log.trace("Get counter value for property {} of entity {}", columnName.get(0, STRING_SRZ),
				context.getEntityClass().getCanonicalName());
		return context.executeWithReadConsistencyLevel(new SafeExecutionContext<Long>()
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
		log.trace("Get counter value for property {} of entity {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				readLevel.name());
		validator.validateNoPendingBatch(context);
		return context.executeWithReadConsistencyLevel(new SafeExecutionContext<Long>()
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
		log.trace("Increment counter value for property {} of entity {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName());

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace("Increment counter value for property {} of entity {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				writeLevel);

		validator.validateNoPendingBatch(context);
		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace("Increment counter value for property {} of entity {} of {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				increment);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace(
				"Increment counter value for property {} of entity {} of {}  with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				increment, writeLevel);

		validator.validateNoPendingBatch(context);
		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace("Decrement counter value for property {} of entity {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName());

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace("Decrement counter value for property {} of entity {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				writeLevel);

		validator.validateNoPendingBatch(context);
		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace("Decrement counter value for property {} of entity {} of {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				decrement);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
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
		log.trace(
				"Decrement counter value for property {} of entity {} pof {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(),
				decrement, writeLevel);

		validator.validateNoPendingBatch(context);
		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		}, writeLevel);
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
