package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.AchillesArgumentExtractor;
import info.archinnov.achilles.configuration.CQLArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;

import java.util.Map;

import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManagerFactory extends AchillesEntityManagerFactory
{
	private static final Logger log = LoggerFactory.getLogger(CQLEntityManagerFactory.class);

	public CQLEntityManagerFactory(Map<String, Object> configurationMap) {

		super(configurationMap, new CQLArgumentExtractor());

	}

	@Override
	public EntityManager createEntityManager()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EntityManager createEntityManager(Map map)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(
			Map<String, Object> configurationMap, AchillesArgumentExtractor argumentExtractor)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
