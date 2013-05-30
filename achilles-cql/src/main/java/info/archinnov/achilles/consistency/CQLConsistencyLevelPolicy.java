package info.archinnov.achilles.consistency;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

/**
 * CQLConsistencyLevelPolicy
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLConsistencyLevelPolicy extends AchillesConsistencyLevelPolicy
{

	public CQLConsistencyLevelPolicy(ConsistencyLevel defaultGlobalReadConsistencyLevel,
			ConsistencyLevel defaultGlobalWriteConsistencyLevel,
			Map<String, ConsistencyLevel> readCfConsistencyLevels,
			Map<String, ConsistencyLevel> writeCfConsistencyLevels)
	{
		super(defaultGlobalReadConsistencyLevel, defaultGlobalWriteConsistencyLevel,
				readCfConsistencyLevels, writeCfConsistencyLevels);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#loadConsistencyLevelForRead(java.lang.String)
	 */
	@Override
	public void loadConsistencyLevelForRead(String columnFamily)
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#loadConsistencyLevelForWrite(java.lang.String)
	 */
	@Override
	public void loadConsistencyLevelForWrite(String columnFamily)
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#reinitDefaultConsistencyLevels()
	 */
	@Override
	public void reinitDefaultConsistencyLevels()
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#reinitCurrentConsistencyLevels()
	 */
	@Override
	public void reinitCurrentConsistencyLevels()
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#getCurrentReadLevel()
	 */
	@Override
	public ConsistencyLevel getCurrentReadLevel()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#setCurrentReadLevel(info.archinnov.achilles.type.ConsistencyLevel)
	 */
	@Override
	public void setCurrentReadLevel(ConsistencyLevel readLevel)
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#removeCurrentReadLevel()
	 */
	@Override
	public void removeCurrentReadLevel()
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#getCurrentWriteLevel()
	 */
	@Override
	public ConsistencyLevel getCurrentWriteLevel()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#setCurrentWriteLevel(info.archinnov.achilles.type.ConsistencyLevel)
	 */
	@Override
	public void setCurrentWriteLevel(ConsistencyLevel writeLevel)
	{
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy#removeCurrentWriteLevel()
	 */
	@Override
	public void removeCurrentWriteLevel()
	{
		// TODO Auto-generated method stub

	}

}
