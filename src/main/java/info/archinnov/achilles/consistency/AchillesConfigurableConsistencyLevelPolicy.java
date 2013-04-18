package info.archinnov.achilles.consistency;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesConfigurableConsistencyLevelPolicy
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesConfigurableConsistencyLevelPolicy extends ConfigurableConsistencyLevel
{
	private static final Logger log = LoggerFactory
			.getLogger(AchillesConfigurableConsistencyLevelPolicy.class);

	static final ThreadLocal<HConsistencyLevel> defaultReadConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();
	static final ThreadLocal<HConsistencyLevel> defaultWriteConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();
	static final ThreadLocal<ConsistencyLevel> currentReadConsistencyLevel = new ThreadLocal<ConsistencyLevel>();
	static final ThreadLocal<ConsistencyLevel> currentWriteConsistencyLevel = new ThreadLocal<ConsistencyLevel>();

	public AchillesConfigurableConsistencyLevelPolicy(ConsistencyLevel defaultReadLevel,
			ConsistencyLevel defaultWriteLevel, Map<String, HConsistencyLevel> readConsistencyMap,
			Map<String, HConsistencyLevel> writeConsistencyMap)
	{
		super();

		log.debug(
				"Initializing Achilles Configurable Consistency Level Policy with default read/write levels {}/{} and read/write level maps {}/{}",
				defaultReadLevel, defaultWriteLevel, readConsistencyMap, writeConsistencyMap);

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
				log.trace("Set default read consistency level to {} in the thread {}",
						result.name(), Thread.currentThread());
				break;
			case WRITE:
				result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
						.get() : super.get(op);
				log.trace("Set default write consistency level to {} in the thread {}",
						result.name(), Thread.currentThread());
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
				log.trace(
						"Set default read consistency of column family {} level to {} in the thread {}",
						result.name(), columnFamily, Thread.currentThread());
				break;
			case WRITE:
				result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
						.get() : super.get(OperationType.WRITE, columnFamily);
				log.trace(
						"Set default write consistency of column family {} level to {} in the thread {}",
						result.name(), columnFamily, Thread.currentThread());
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
			log.trace(
					"Load default read consistency of column family {} level to {} in the thread {}",
					currentLevel.name(), columnFamily, Thread.currentThread());
		}
		else
		{
			HConsistencyLevel level = this.get(OperationType.READ, columnFamily);
			defaultReadConsistencyLevelTL.set(level);
			log.trace(
					"Load default read consistency of column family {} level to {} in the thread {}",
					level.name(), columnFamily, Thread.currentThread());
		}
	}

	public <T> void loadConsistencyLevelForWrite(String columnFamily)
	{
		ConsistencyLevel currentLevel = currentWriteConsistencyLevel.get();
		if (currentLevel != null)
		{
			defaultWriteConsistencyLevelTL.set(currentLevel.getHectorLevel());
			log.trace(
					"Load default write consistency of column family {} level to {} in the thread {}",
					currentLevel.name(), columnFamily, Thread.currentThread());
		}
		else
		{
			HConsistencyLevel level = this.get(OperationType.WRITE, columnFamily);
			defaultWriteConsistencyLevelTL.set(level);
			log.trace(
					"Load default write consistency of column family {} level to {} in the thread {}",
					level.name(), columnFamily, Thread.currentThread());
		}
	}

	public void reinitDefaultConsistencyLevels()
	{
		log.trace("Reinit defaut read/write consistency levels in the thread {}",
				Thread.currentThread());
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
	}

	public void reinitCurrentConsistencyLevels()
	{
		log.trace("Reinit current read/write consistency levels in the thread {}", Thread
				.currentThread().getId());
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
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

	public ConsistencyLevel getCurrentReadLevel()
	{
		log.trace("Current read consistency level is {} in the thread {}",
				currentReadConsistencyLevel.get(), Thread.currentThread());
		return currentReadConsistencyLevel.get();
	}

	public void setCurrentReadLevel(ConsistencyLevel readLevel)
	{
		log.trace("Set current read consistency level to {} in the thread {}", readLevel, Thread
				.currentThread().getId());
		currentReadConsistencyLevel.set(readLevel);
	}

	public void removeCurrentReadLevel()
	{
		log.trace("Remove current read consistency level  in the thread {}", Thread.currentThread()
				.getId());
		currentReadConsistencyLevel.remove();
	}

	public ConsistencyLevel getCurrentWriteLevel()
	{
		log.trace("Current write consistency level is {} in the thread {}",
				currentReadConsistencyLevel.get(), Thread.currentThread());
		return currentWriteConsistencyLevel.get();
	}

	public void setCurrentWriteLevel(ConsistencyLevel writeLevel)
	{
		log.trace("Set current write consistency level to {} in the thread {}", writeLevel, Thread
				.currentThread().getId());
		currentWriteConsistencyLevel.set(writeLevel);
	}

	public void removeCurrentWriteLevel()
	{
		log.trace("Remove current write consistency level  in the thread {}", Thread
				.currentThread().getId());
		currentWriteConsistencyLevel.remove();
	}
}
