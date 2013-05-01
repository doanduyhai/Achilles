package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

/**
 * DaoContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class DaoContext
{
	private final Map<String, GenericEntityDao<?>> entityDaosMap;
	private final Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	private final CounterDao counterDao;

	public DaoContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao)
	{
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
	}

	public CounterDao getCounterDao()
	{
		return counterDao;
	}

	public GenericEntityDao<?> findEntityDao(String columnFamilyName)
	{
		return entityDaosMap.get(columnFamilyName);
	}

	public GenericColumnFamilyDao<?, ?> findColumnFamilyDao(String columnFamilyName)
	{
		return columnFamilyDaosMap.get(columnFamilyName);
	}
}
