package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.AchillesArgumentExtractor;
import info.archinnov.achilles.configuration.CQLArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.CQLConsistencyLevelPolicy;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * CQLEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManagerFactory extends AchillesEntityManagerFactory
{
	private static final Logger log = LoggerFactory.getLogger(CQLEntityManagerFactory.class);
	private Cluster cluster;
	private Session session;
	private CQLDaoContext daoContext;

	public CQLEntityManagerFactory(Map<String, Object> configurationMap) {
		super(configurationMap, new CQLArgumentExtractor());
		CQLArgumentExtractor extractor = new CQLArgumentExtractor();
		cluster = extractor.initCluster(configurationMap);
		session = extractor.initSession(cluster, configurationMap);
		boolean hasSimpleCounter = bootstrap();

	}

	@Override
	public CqlEntityManager createEntityManager()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CqlEntityManager createEntityManager(Map map)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(
			Map<String, Object> configurationMap, AchillesArgumentExtractor argumentExtractor)
	{
		log.info("Initializing new Achilles Configurable Consistency Level Policy from arguments ");

		ConsistencyLevel defaultReadConsistencyLevel = argumentExtractor
				.initDefaultReadConsistencyLevel(configurationMap);
		ConsistencyLevel defaultWriteConsistencyLevel = argumentExtractor
				.initDefaultWriteConsistencyLevel(configurationMap);
		Map<String, ConsistencyLevel> readConsistencyMap = argumentExtractor
				.initReadConsistencyMap(configurationMap);
		Map<String, ConsistencyLevel> writeConsistencyMap = argumentExtractor
				.initWriteConsistencyMap(configurationMap);

		return new CQLConsistencyLevelPolicy(defaultReadConsistencyLevel,
				defaultWriteConsistencyLevel, readConsistencyMap, writeConsistencyMap);
	}

}
