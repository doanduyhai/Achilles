package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.type.Pair;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCounterDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterDao extends ThriftAbstractDao
{
	private static final Logger log = LoggerFactory.getLogger(ThriftCounterDao.class);
	public static final String COUNTER_CF = "achillesCounterCF";

	public ThriftCounterDao(Cluster cluster, Keyspace keyspace,
			AchillesConsistencyLevelPolicy consistencyPolicy, Pair<?, ?> rowkeyAndValueClasses)
	{
		super(cluster, keyspace, COUNTER_CF, consistencyPolicy, rowkeyAndValueClasses);

		columnNameSerializer = COMPOSITE_SRZ;
		log
				.debug("Initializing CounterDao with Composite key serializer, DynamicComposite comparator and Long value serializer ");
	}
}
