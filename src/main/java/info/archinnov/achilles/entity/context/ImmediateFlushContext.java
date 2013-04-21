package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.context.FlushContext.FlushType.IMMEDIATE;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ImmediateFlushContext extends AbstractFlushContext
{
	private static final Logger log = LoggerFactory.getLogger(ImmediateFlushContext.class);

	public ImmediateFlushContext(Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao,
			AchillesConfigurableConsistencyLevelPolicy policy)
	{
		super(entityDaosMap, columnFamilyDaosMap, counterDao, policy);
	}

	@Override
	public void startBatch()
	{
		throw new UnsupportedOperationException(
				"Cannot start a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public void flush()
	{
		log.debug("Flush immediatly all pending mutations");
		doFlush();
	}

	@Override
	public void endBatch()
	{
		throw new UnsupportedOperationException(
				"Cannot end a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public FlushType type()
	{
		return IMMEDIATE;
	}
}
