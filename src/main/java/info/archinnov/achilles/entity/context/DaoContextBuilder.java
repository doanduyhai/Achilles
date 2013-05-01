package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DaoBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class DaoContextBuilder
{
	private static final Logger log = LoggerFactory.getLogger(DaoContextBuilder.class);

	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();
	private CounterDao counterDao;

	public DaoContext buildDao(Cluster cluster, Keyspace keyspace,
			Map<Class<?>, EntityMeta<?>> entityMetaMap, ConfigurationContext configContext,
			boolean hasSimpleCounter)
	{
		if (hasSimpleCounter)
		{
			counterDao = new CounterDao(cluster, keyspace, configContext.getConsistencyPolicy());
			log.debug("Build achillesCounterCF dao");
		}

		for (EntityMeta<?> entityMeta : entityMetaMap.values())
		{
			if (!entityMeta.isColumnFamilyDirectMapping())
			{
				buildEntityDao(cluster, keyspace, configContext, entityMeta);
			}

			for (PropertyMeta<?, ?> propertyMeta : entityMeta.getPropertyMetas().values())
			{
				if (propertyMeta.type().isWideMap())
				{
					if (propertyMeta.type().isJoinColumn())
					{
						EntityMeta<?> joinEntityMeta = propertyMeta.getJoinProperties()
								.getEntityMeta();
						buildColumnFamilyDaoForJoinWideMap(cluster, keyspace, configContext,
								propertyMeta, joinEntityMeta);
					}
					else
					{
						buildColumnFamilyDao(cluster, keyspace, configContext, entityMeta,
								propertyMeta);
					}
				}
			}
		}
		return new DaoContext(entityDaosMap, columnFamilyDaosMap, counterDao);
	}

	private <ID> void buildEntityDao(Cluster cluster, Keyspace keyspace,
			ConfigurationContext configContext, EntityMeta<ID> entityMeta)
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		GenericEntityDao<ID> entityDao = new GenericEntityDao<ID>(//
				cluster, //
				keyspace, //
				(Serializer<ID>) entityMeta.getIdMeta().getValueSerializer(), //
				columnFamilyName, //
				configContext.getConsistencyPolicy());

		entityDaosMap.put(columnFamilyName, entityDao);
		log.debug("Build entity dao for column family {}", columnFamilyName);
	}

	private <ID, K, V> void buildColumnFamilyDao(Cluster cluster, Keyspace keyspace,
			ConfigurationContext configContext, EntityMeta<ID> entityMeta,
			PropertyMeta<K, V> propertyMeta)
	{
		Class<?> valueClass = propertyMeta.getValueClass();
		GenericColumnFamilyDao<?, ?> dao;

		PropertyMeta<Void, ID> idMeta = entityMeta.getIdMeta();
		String externalCFName = propertyMeta.getExternalCFName();
		AchillesConfigurableConsistencyLevelPolicy consistencyPolicy = configContext
				.getConsistencyPolicy();
		if (isSupportedType(valueClass))
		{
			dao = new GenericColumnFamilyDao<ID, V>(cluster, keyspace, //
					idMeta.getValueSerializer(), //
					propertyMeta.getValueSerializer(), //
					externalCFName, consistencyPolicy);
		}
		else if (Counter.class.isAssignableFrom(valueClass))
		{
			dao = new GenericColumnFamilyDao<ID, Long>(cluster, keyspace, //
					idMeta.getValueSerializer(), //
					LONG_SRZ, //
					externalCFName, consistencyPolicy);
		}
		else
		{
			dao = new GenericColumnFamilyDao<ID, String>(cluster, keyspace, //
					idMeta.getValueSerializer(), //
					STRING_SRZ, //
					externalCFName, consistencyPolicy);
		}
		columnFamilyDaosMap.put(externalCFName, dao);
		log.debug("Build column family dao for column family {}", externalCFName);

	}

	@SuppressWarnings("unchecked")
	private <ID, K, V, JOIN_ID> void buildColumnFamilyDaoForJoinWideMap(Cluster cluster,
			Keyspace keyspace, ConfigurationContext configContext, PropertyMeta<K, V> propertyMeta,
			EntityMeta<JOIN_ID> joinEntityMeta)
	{

		GenericColumnFamilyDao<ID, JOIN_ID> joinDao = new GenericColumnFamilyDao<ID, JOIN_ID>(
				cluster, //
				keyspace, //
				(Serializer<ID>) propertyMeta.getIdSerializer(), //
				joinEntityMeta.getIdSerializer(), //
				propertyMeta.getExternalCFName(),//
				configContext.getConsistencyPolicy());

		log.debug("Building join dao for column family {}", propertyMeta.getExternalCFName());

		columnFamilyDaosMap.put(propertyMeta.getExternalCFName(), joinDao);
	}
}
