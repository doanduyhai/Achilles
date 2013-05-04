package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
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
public class GenericWideRowDao<K, V> extends AbstractDao<K, V>
{

	private static final Logger log = LoggerFactory.getLogger(GenericWideRowDao.class);

	public GenericWideRowDao(Cluster cluster, Keyspace keyspace, //
			Serializer<K> keySrz, //
			Serializer<V> valSrz, //
			String cf, //
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{

		super(cluster, keyspace);

		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = valSrz;
		policy = consistencyPolicy;
		log.debug(
				"Initializing GenericColumnFamilyDao for key serializer '{}', composite comparator and value serializer '{}'",
				keySrz.getComparatorType().getTypeName(), valSrz.getComparatorType().getTypeName());
	}
}
