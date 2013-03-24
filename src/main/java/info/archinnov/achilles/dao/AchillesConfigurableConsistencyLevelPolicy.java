package info.archinnov.achilles.dao;

import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentReadConsistencyLevel;
import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentWriteConsistencyLevel;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.HConsistencyLevel;

/**
 * AchillesConfigurableConsistencyLevelPolicy
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesConfigurableConsistencyLevelPolicy extends ConfigurableConsistencyLevel
{

	static final ThreadLocal<HConsistencyLevel> defaultReadConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();
	static final ThreadLocal<HConsistencyLevel> defaultWriteConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();

	public AchillesConfigurableConsistencyLevelPolicy(ConsistencyLevel defaultReadLevel,
			ConsistencyLevel defaultWriteLevel, Map<String, HConsistencyLevel> readConsistencyMap,
			Map<String, HConsistencyLevel> writeConsistencyMap)
	{
		super();
		this.setDefaultReadConsistencyLevel(defaultReadLevel.getHectorLevel());
		this.setDefaultWriteConsistencyLevel(defaultWriteLevel.getHectorLevel());
		this.setReadCfConsistencyLevels(readConsistencyMap);
		this.setWriteCfConsistencyLevels(writeConsistencyMap);
	}

	@Override
	public HConsistencyLevel get(OperationType op)
	{
		HConsistencyLevel result = super.get(op);
		switch (op)
		{
			case READ:
				result = defaultReadConsistencyLevelTL.get() != null ? defaultReadConsistencyLevelTL
						.get() : super.get(op);
				break;
			case WRITE:
				result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
						.get() : super.get(op);
				break;
			default:
				result = super.get(op);
		}

		return result;
	}

	@Override
	public HConsistencyLevel get(OperationType op, String columnFamily)
	{
		HConsistencyLevel result;
		switch (op)
		{
			case READ:
				result = defaultReadConsistencyLevelTL.get() != null ? defaultReadConsistencyLevelTL
						.get() : super.get(OperationType.READ, columnFamily);
				break;
			case WRITE:
				result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
						.get() : super.get(OperationType.WRITE, columnFamily);
				break;
			default:
				result = super.get(op);
		}
		return result;
	}

	public <T> void loadConsistencyLevelForRead(String columnFamily)
	{
		ConsistencyLevel currentLevel = currentReadConsistencyLevel.get();
		if (currentLevel != null)
		{
			defaultReadConsistencyLevelTL.set(currentLevel.getHectorLevel());
		}
		else
		{
			defaultReadConsistencyLevelTL.set(this.get(OperationType.READ, columnFamily));
		}
	}

	public <T> void loadConsistencyLevelForWrite(String columnFamily)
	{
		ConsistencyLevel currentLevel = currentWriteConsistencyLevel.get();
		if (currentLevel != null)
		{
			defaultWriteConsistencyLevelTL.set(currentLevel.getHectorLevel());
		}
		else
		{
			defaultWriteConsistencyLevelTL.set(this.get(OperationType.WRITE, columnFamily));
		}
	}

	public void reinitDefaultConsistencyLevel()
	{
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
	}

	public HConsistencyLevel getConsistencyLevelForRead(String columnFamily)
	{
		return this.get(OperationType.READ, columnFamily);
	}

	public void setConsistencyLevelForRead(HConsistencyLevel consistencyLevel, String columnFamily)
	{
		this.setConsistencyLevelForCfOperation(consistencyLevel, columnFamily, OperationType.READ);
	}

	public HConsistencyLevel getConsistencyLevelForWrite(String columnFamily)
	{
		return this.get(OperationType.WRITE, columnFamily);
	}

	public void setConsistencyLevelForWrite(HConsistencyLevel consistencyLevel, String columnFamily)
	{
		this.setConsistencyLevelForCfOperation(consistencyLevel, columnFamily, OperationType.WRITE);
	}

}
