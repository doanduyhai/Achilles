package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.CQLArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.CQLConsistencyLevelPolicy;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLDaoContextBuilder;
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
public class CQLEntityManagerFactory extends EntityManagerFactory
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

		daoContext = CQLDaoContextBuilder.builder(session).build(entityMetaMap);

	}

	public CQLEntityManager createEntityManager()
	{
		return new CQLEntityManager(this, entityMetaMap, configContext, daoContext);
	}

	@Override
	protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(
			Map<String, Object> configurationMap, ArgumentExtractor argumentExtractor)
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
