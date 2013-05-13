package info.archinnov.achilles.helper;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.HConsistencyLevel;

/**
 * ThriftConsistencyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyHelper
{
	private final static Map<ConsistencyLevel, HConsistencyLevel> fromAchillesToHector = new HashMap<ConsistencyLevel, HConsistencyLevel>();

	static
	{
		fromAchillesToHector.put(ConsistencyLevel.ANY, HConsistencyLevel.ANY);
		fromAchillesToHector.put(ConsistencyLevel.ONE, HConsistencyLevel.ONE);
		fromAchillesToHector.put(ConsistencyLevel.TWO, HConsistencyLevel.TWO);
		fromAchillesToHector.put(ConsistencyLevel.THREE, HConsistencyLevel.THREE);
		fromAchillesToHector.put(ConsistencyLevel.QUORUM, HConsistencyLevel.QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.LOCAL_QUORUM, HConsistencyLevel.LOCAL_QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.EACH_QUORUM, HConsistencyLevel.EACH_QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.ALL, HConsistencyLevel.ALL);
	}

	public static HConsistencyLevel getHectorLevel(ConsistencyLevel achillesLevel)
	{
		HConsistencyLevel hectorLevel = fromAchillesToHector.get(achillesLevel);
		if (hectorLevel == null)
		{
			throw new IllegalArgumentException(
					"No matching Hector Consistency Level for Achilles level '"
							+ achillesLevel.name() + "'");
		}

		return hectorLevel;
	}

}
