package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
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
public class CounterDao extends AbstractDao<Composite, Long>
{
	private static final Logger log = LoggerFactory.getLogger(CounterDao.class);
	public static final String COUNTER_CF = "achillesCounterCF";

	public CounterDao(Cluster cluster, Keyspace keyspace,
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{

		super(cluster, keyspace);

		keySerializer = COMPOSITE_SRZ;
		columnFamily = COUNTER_CF;
		columnNameSerializer = COMPOSITE_SRZ;
		valueSerializer = LONG_SRZ;
		policy = consistencyPolicy;
		log.debug("Initializing CounterDao with Composite key serializer, DynamicComposite comparator and Long value serializer ");
	}
}
