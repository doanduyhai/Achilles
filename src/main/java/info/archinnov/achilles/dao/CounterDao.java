package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CounterDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterDao extends AbstractDao<Composite, DynamicComposite, Long>
{
	private static final Logger log = LoggerFactory.getLogger(CounterDao.class);
	public static final String COUNTER_CF = "achillesCounterCF";

	public CounterDao(Keyspace keyspace,
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{

		super(keyspace);

		keySerializer = COMPOSITE_SRZ;
		columnFamily = COUNTER_CF;
		columnNameSerializer = DYNA_COMP_SRZ;
		valueSerializer = LONG_SRZ;
		policy = consistencyPolicy;
		log.debug("Initializing CounterDao with Composite key serializer, DynamicComposite comparator and Long value serializer ");
	}
}
