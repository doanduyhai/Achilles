package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericColumnFamilyDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftGenericWideRowDao<K, V> extends ThriftAbstractDao<K, V>
{

	private static final Logger log = LoggerFactory.getLogger(ThriftGenericWideRowDao.class);

	public ThriftGenericWideRowDao(Cluster cluster, Keyspace keyspace, //
			Serializer<K> keySrz, //
			Serializer<V> valSrz, //
			String cf, //
			AchillesConsistencyLevelPolicy consistencyPolicy)
	{

		super(cluster, keyspace, consistencyPolicy);

		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = valSrz;
		log.debug(
				"Initializing GenericColumnFamilyDao for key serializer '{}', composite comparator and value serializer '{}'",
				keySrz.getComparatorType().getTypeName(), valSrz.getComparatorType().getTypeName());
	}
}
