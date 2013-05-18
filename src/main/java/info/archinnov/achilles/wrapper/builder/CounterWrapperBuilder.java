package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.wrapper.CounterWrapper;
import me.prettyprint.hector.api.beans.Composite;

/**
 * CounterWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWrapperBuilder
{
	private Object key;
	private Composite columnName;
	private ThriftAbstractDao counterDao;
	private ThriftPersistenceContext context;
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;

	public static CounterWrapperBuilder builder(ThriftPersistenceContext context)
	{
		return new CounterWrapperBuilder(context);
	}

	protected CounterWrapperBuilder(ThriftPersistenceContext context) {
		this.context = context;
	}

	public CounterWrapperBuilder key(Object key)
	{
		this.key = key;
		return this;
	}

	public CounterWrapperBuilder counterDao(ThriftAbstractDao counterDao)
	{
		this.counterDao = counterDao;
		return this;
	}

	public CounterWrapperBuilder columnName(Composite columnName)
	{
		this.columnName = columnName;
		return this;
	}

	public CounterWrapperBuilder readLevel(ConsistencyLevel readLevel)
	{
		this.readLevel = readLevel;
		return this;
	}

	public CounterWrapperBuilder writeLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
		return this;
	}

	public CounterWrapper build()
	{
		CounterWrapper wrapper = new CounterWrapper(context);
		wrapper.setCounterDao(counterDao);
		wrapper.setColumnName(columnName);
		wrapper.setReadLevel(readLevel);
		wrapper.setWriteLevel(writeLevel);
		wrapper.setKey(key);
		return wrapper;
	}
}
