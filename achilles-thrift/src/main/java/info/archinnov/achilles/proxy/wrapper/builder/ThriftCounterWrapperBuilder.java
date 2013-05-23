package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;

/**
 * ThriftCounterWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterWrapperBuilder
{
	private Object key;
	private Composite columnName;
	private ThriftAbstractDao counterDao;
	private ThriftPersistenceContext context;
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;

	public static ThriftCounterWrapperBuilder builder(ThriftPersistenceContext context)
	{
		return new ThriftCounterWrapperBuilder(context);
	}

	protected ThriftCounterWrapperBuilder(ThriftPersistenceContext context) {
		this.context = context;
	}

	public ThriftCounterWrapperBuilder key(Object key)
	{
		this.key = key;
		return this;
	}

	public ThriftCounterWrapperBuilder counterDao(ThriftAbstractDao counterDao)
	{
		this.counterDao = counterDao;
		return this;
	}

	public ThriftCounterWrapperBuilder columnName(Composite columnName)
	{
		this.columnName = columnName;
		return this;
	}

	public ThriftCounterWrapperBuilder readLevel(ConsistencyLevel readLevel)
	{
		this.readLevel = readLevel;
		return this;
	}

	public ThriftCounterWrapperBuilder writeLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
		return this;
	}

	public ThriftCounterWrapper build()
	{
		ThriftCounterWrapper wrapper = new ThriftCounterWrapper(context);
		wrapper.setCounterDao(counterDao);
		wrapper.setColumnName(columnName);
		wrapper.setReadLevel(readLevel);
		wrapper.setWriteLevel(writeLevel);
		wrapper.setKey(key);
		return wrapper;
	}
}
