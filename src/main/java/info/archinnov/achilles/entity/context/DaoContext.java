package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
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
	private final Map<String, GenericWideRowDao<?, ?>> wideRowDaosMap;
	private final CounterDao counterDao;

	public DaoContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericWideRowDao<?, ?>> wideRowDaosMap, CounterDao counterDao)
	{
		this.entityDaosMap = entityDaosMap;
		this.wideRowDaosMap = wideRowDaosMap;
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

	public GenericWideRowDao<?, ?> findWideRowDao(String columnFamilyName)
	{
		return wideRowDaosMap.get(columnFamilyName);
	}
}
