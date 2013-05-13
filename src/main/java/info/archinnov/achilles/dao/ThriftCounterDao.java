package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CounterDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterDao extends ThriftAbstractDao<Composite, Long>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftCounterDao.class);
	public static final String COUNTER_CF = "achillesCounterCF";

	public ThriftCounterDao(Cluster cluster, Keyspace keyspace,
			AchillesConsistencyLevelPolicy consistencyPolicy)
	{

		super(cluster, keyspace, consistencyPolicy);

		keySerializer = COMPOSITE_SRZ;
		columnFamily = COUNTER_CF;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = LONG_SRZ;
		log.debug("Initializing CounterDao with Composite key serializer, DynamicComposite comparator and Long value serializer ");
	}
}
