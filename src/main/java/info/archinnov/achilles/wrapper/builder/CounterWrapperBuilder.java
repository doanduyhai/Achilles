package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.wrapper.CounterWrapper;
import me.prettyprint.hector.api.beans.Composite;

/**
 * CounterWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWrapperBuilder<ID>
{
	private ID key;
	private Composite columnName;
	private AbstractDao<ID, Long> counterDao;
	private PersistenceContext<?> context;
	private ConsistencyLevel readLevel;
	private ConsistencyLevel writeLevel;

	public static <ID> CounterWrapperBuilder<ID> builder(ID key)
	{
		return new CounterWrapperBuilder<ID>(key);
	}

	public CounterWrapperBuilder(ID key) {
		this.key = key;
	}

	public CounterWrapperBuilder<ID> counterDao(AbstractDao<ID, Long> counterDao)
	{
		this.counterDao = counterDao;
		return this;
	}

	public CounterWrapperBuilder<ID> columnName(Composite columnName)
	{
		this.columnName = columnName;
		return this;
	}

	public CounterWrapperBuilder<ID> readLevel(ConsistencyLevel readLevel)
	{
		this.readLevel = readLevel;
		return this;
	}

	public CounterWrapperBuilder<ID> writeLevel(ConsistencyLevel writeLevel)
	{
		this.writeLevel = writeLevel;
		return this;
	}

	public CounterWrapperBuilder<ID> context(PersistenceContext<?> context)
	{
		this.context = context;
		return this;
	}

	public CounterWrapper<ID> build()
	{
		CounterWrapper<ID> wrapper = new CounterWrapper<ID>(key);
		wrapper.setCounterDao(counterDao);
		wrapper.setColumnName(columnName);
		wrapper.setReadLevel(readLevel);
		wrapper.setWriteLevel(writeLevel);
		wrapper.setContext(context);
		return wrapper;
	}
}
