package info.archinnov.achilles.helper;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

/**
 * CQLConsistencyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLConsistencyHelper
{
	private final static Map<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel> fromAchillesToCQL = new HashMap<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel>();

	static
	{
		fromAchillesToCQL.put(ConsistencyLevel.ANY, com.datastax.driver.core.ConsistencyLevel.ANY);
		fromAchillesToCQL.put(ConsistencyLevel.ONE, com.datastax.driver.core.ConsistencyLevel.ONE);
		fromAchillesToCQL.put(ConsistencyLevel.TWO, com.datastax.driver.core.ConsistencyLevel.TWO);
		fromAchillesToCQL.put(ConsistencyLevel.THREE,
				com.datastax.driver.core.ConsistencyLevel.THREE);
		fromAchillesToCQL.put(ConsistencyLevel.QUORUM,
				com.datastax.driver.core.ConsistencyLevel.QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.LOCAL_QUORUM,
				com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.EACH_QUORUM,
				com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.ALL, com.datastax.driver.core.ConsistencyLevel.ALL);
	}

	public static com.datastax.driver.core.ConsistencyLevel getCQLLevel(
			ConsistencyLevel achillesLevel)
	{
		com.datastax.driver.core.ConsistencyLevel cqlLevel = fromAchillesToCQL.get(achillesLevel);
		if (cqlLevel == null)
		{
			throw new IllegalArgumentException(
					"No matching Hector Consistency Level for Achilles level '"
							+ achillesLevel.name() + "'");
		}

		return cqlLevel;
	}

}
