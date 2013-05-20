package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.Pair;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DaoBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftDaoContextBuilder
{
	private static final Logger log = LoggerFactory.getLogger(ThriftDaoContextBuilder.class);

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();
	private ThriftCounterDao thriftCounterDao;

	public ThriftDaoContext buildDao(Cluster cluster, Keyspace keyspace,
			Map<Class<?>, EntityMeta> entityMetaMap, AchillesConfigurationContext configContext,
			boolean hasSimpleCounter)
	{
		if (hasSimpleCounter)
		{
			thriftCounterDao = new ThriftCounterDao(cluster, keyspace,
					configContext.getConsistencyPolicy(), //
					new Pair<Class<Composite>, Class<Long>>(Composite.class, Long.class));
			log.debug("Build achillesCounterCF dao");
		}

		for (EntityMeta entityMeta : entityMetaMap.values())
		{
			if (!entityMeta.isWideRow())
			{
				buildEntityDao(cluster, keyspace, configContext, entityMeta);
			}

			for (PropertyMeta<?, ?> propertyMeta : entityMeta.getPropertyMetas().values())
			{
				if (propertyMeta.type().isWideMap())
				{
					if (propertyMeta.type().isJoinColumn())
					{
						EntityMeta joinEntityMeta = propertyMeta.getJoinProperties()
								.getEntityMeta();
						buildWideRowDaoForJoinWideMap(cluster, keyspace, configContext,
								propertyMeta, joinEntityMeta);
					}
					else
					{
						buildWideRowDao(cluster, keyspace, configContext, entityMeta, propertyMeta);
					}
				}
			}
		}
		return new ThriftDaoContext(entityDaosMap, wideRowDaosMap, thriftCounterDao);
	}

	private void buildEntityDao(Cluster cluster, Keyspace keyspace,
			AchillesConfigurationContext configContext, EntityMeta entityMeta)
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		ThriftGenericEntityDao entityDao = new ThriftGenericEntityDao(//
				cluster, //
				keyspace, //
				columnFamilyName, //
				configContext.getConsistencyPolicy(), //
				new Pair<Class<?>, Class<String>>(entityMeta.getIdClass(), String.class));

		entityDaosMap.put(columnFamilyName, entityDao);
		log.debug("Build entity dao for column family {}", columnFamilyName);
	}

	private <K, V> void buildWideRowDao(Cluster cluster, Keyspace keyspace,
			AchillesConfigurationContext configContext, EntityMeta entityMeta,
			PropertyMeta<K, V> propertyMeta)
	{
		Class<?> valueClass = propertyMeta.getValueClass();
		ThriftGenericWideRowDao dao;

		String externalCFName = propertyMeta.getExternalCFName();
		AchillesConsistencyLevelPolicy consistencyPolicy = configContext.getConsistencyPolicy();
		if (isSupportedType(valueClass))
		{
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					externalCFName, consistencyPolicy, //
					new Pair<Class<?>, Class<V>>(entityMeta.getIdClass(),
							propertyMeta.getValueClass()));
		}
		else if (Counter.class.isAssignableFrom(valueClass))
		{
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					externalCFName, consistencyPolicy,//
					new Pair<Class<?>, Class<Long>>(entityMeta.getIdClass(), Long.class));
		}
		else
		{
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					externalCFName, consistencyPolicy, //
					new Pair<Class<?>, Class<String>>(entityMeta.getIdClass(), String.class));
		}
		wideRowDaosMap.put(externalCFName, dao);
		log.debug("Build column family dao for wide row {}", externalCFName);

	}

	private <K, V> void buildWideRowDaoForJoinWideMap(Cluster cluster, Keyspace keyspace,
			AchillesConfigurationContext configContext, PropertyMeta<K, V> propertyMeta,
			EntityMeta joinEntityMeta)
	{

		ThriftGenericWideRowDao joinDao = new ThriftGenericWideRowDao(
				cluster, //
				keyspace, //
				propertyMeta.getExternalCFName(),//
				(ThriftConsistencyLevelPolicy) configContext.getConsistencyPolicy(),//
				new Pair<Class<?>, Class<?>>(propertyMeta.getIdClass(), joinEntityMeta.getIdClass()));

		log.debug("Building join dao for wide row {}", propertyMeta.getExternalCFName());

		wideRowDaosMap.put(propertyMeta.getExternalCFName(), joinDao);
	}
}
