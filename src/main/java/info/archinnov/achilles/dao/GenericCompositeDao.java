package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericCompositeDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class GenericCompositeDao<K, V> extends AbstractDao<K, Composite, V>
{

	private static final Logger log = LoggerFactory.getLogger(GenericCompositeDao.class);

	public GenericCompositeDao(Keyspace keyspace, Serializer<K> keySrz, Serializer<V> valSrz,
			String cf, AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{

		super(keyspace);

		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = valSrz;
		policy = consistencyPolicy;
		log.debug(
				"Initializing GenericCompositeDao for key serializer '{}', composite comparator and value serializer '{}'",
				keySrz.getComparatorType().getTypeName(), valSrz.getComparatorType().getTypeName());
	}
}
