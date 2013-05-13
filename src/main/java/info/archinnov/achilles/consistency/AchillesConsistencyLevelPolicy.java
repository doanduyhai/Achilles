package info.archinnov.achilles.consistency;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.Map;

/**
 * AchillesConsistencyLevelPolicy
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesConsistencyLevelPolicy
{

	protected ConsistencyLevel defaultGlobalReadConsistencyLevel;
	protected ConsistencyLevel defaultGlobalWriteConsistencyLevel;

	protected Map<String, ConsistencyLevel> readCfConsistencyLevels;
	protected Map<String, ConsistencyLevel> writeCfConsistencyLevels;

	protected AchillesConsistencyLevelPolicy(ConsistencyLevel defaultGlobalReadConsistencyLevel,
			ConsistencyLevel defaultGlobalWriteConsistencyLevel,
			Map<String, ConsistencyLevel> readCfConsistencyLevels,
			Map<String, ConsistencyLevel> writeCfConsistencyLevels)
	{
		this.defaultGlobalReadConsistencyLevel = defaultGlobalReadConsistencyLevel;
		this.defaultGlobalWriteConsistencyLevel = defaultGlobalWriteConsistencyLevel;
		this.readCfConsistencyLevels = readCfConsistencyLevels;
		this.writeCfConsistencyLevels = writeCfConsistencyLevels;
	}

	public ConsistencyLevel getConsistencyLevelForRead(String columnFamily)
	{
		return readCfConsistencyLevels.get(columnFamily) != null ? readCfConsistencyLevels
				.get(columnFamily) : defaultGlobalReadConsistencyLevel;
	}

	public void setConsistencyLevelForRead(ConsistencyLevel consistencyLevel, String columnFamily)
	{
		readCfConsistencyLevels.put(columnFamily, consistencyLevel);
	}

	public ConsistencyLevel getConsistencyLevelForWrite(String columnFamily)
	{
		return writeCfConsistencyLevels.get(columnFamily) != null ? writeCfConsistencyLevels
				.get(columnFamily) : defaultGlobalWriteConsistencyLevel;
	}

	public void setConsistencyLevelForWrite(ConsistencyLevel consistencyLevel, String columnFamily)
	{
		writeCfConsistencyLevels.put(columnFamily, consistencyLevel);
	}

	public ConsistencyLevel getDefaultGlobalReadConsistencyLevel()
	{
		return defaultGlobalReadConsistencyLevel;
	}

	public ConsistencyLevel getDefaultGlobalWriteConsistencyLevel()
	{
		return defaultGlobalWriteConsistencyLevel;
	}

	public abstract void loadConsistencyLevelForRead(String columnFamily);

	public abstract void loadConsistencyLevelForWrite(String columnFamily);

	public abstract void reinitDefaultConsistencyLevels();

	public abstract void reinitCurrentConsistencyLevels();

	public abstract ConsistencyLevel getCurrentReadLevel();

	public abstract void setCurrentReadLevel(ConsistencyLevel readLevel);

	public abstract void removeCurrentReadLevel();

	public abstract ConsistencyLevel getCurrentWriteLevel();

	public abstract void setCurrentWriteLevel(ConsistencyLevel writeLevel);

	public abstract void removeCurrentWriteLevel();
}
