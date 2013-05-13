package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;

import java.util.Map;

/**
 * DaoContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class DaoContext
{
	private final Map<String, ThriftGenericEntityDao<?>> entityDaosMap;
	private final Map<String, ThriftGenericWideRowDao<?, ?>> wideRowDaosMap;
	private final ThriftCounterDao thriftCounterDao;

	public DaoContext(Map<String, ThriftGenericEntityDao<?>> entityDaosMap,
			Map<String, ThriftGenericWideRowDao<?, ?>> wideRowDaosMap, ThriftCounterDao thriftCounterDao)
	{
		this.entityDaosMap = entityDaosMap;
		this.wideRowDaosMap = wideRowDaosMap;
		this.thriftCounterDao = thriftCounterDao;
	}

	public ThriftCounterDao getCounterDao()
	{
		return thriftCounterDao;
	}

	public ThriftGenericEntityDao<?> findEntityDao(String columnFamilyName)
	{
		return entityDaosMap.get(columnFamilyName);
	}

	public ThriftGenericWideRowDao<?, ?> findWideRowDao(String columnFamilyName)
	{
		return wideRowDaosMap.get(columnFamilyName);
	}
}
