package info.archinnov.achilles.context;

import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftDaoFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftDaoContextBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftDaoContextBuilder
{
	private static final Logger log = LoggerFactory.getLogger(ThriftDaoContextBuilder.class);

	private ThriftDaoFactory daoFactory = new ThriftDaoFactory();

	public ThriftDaoContext buildDao(Cluster cluster, Keyspace keyspace,
			Map<Class<?>, EntityMeta> entityMetaMap,
			ConfigurationContext configContext, boolean hasSimpleCounter)
	{

		Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
		Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();
		ThriftCounterDao thriftCounterDao = null;
		if (hasSimpleCounter)
		{
			thriftCounterDao = daoFactory.createCounterDao(cluster, keyspace, configContext);
			log.debug("Build achillesCounterCF dao");
		}

		for (EntityMeta entityMeta : entityMetaMap.values())
		{
			if (entityMeta.isClusteredEntity())
			{
				daoFactory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta,
						wideRowDaosMap);
			}
			else
			{
				daoFactory.createDaosForEntity(cluster, keyspace, configContext, entityMeta,
						entityDaosMap,
						wideRowDaosMap);
			}
		}
		return new ThriftDaoContext(entityDaosMap, wideRowDaosMap, thriftCounterDao);
	}
}
