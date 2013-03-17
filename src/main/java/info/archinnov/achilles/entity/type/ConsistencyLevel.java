package info.archinnov.achilles.entity.type;

import me.prettyprint.hector.api.HConsistencyLevel;

/**
 * ConsistencyLevel
 * 
 * @author DuyHai DOAN
 * 
 */
public enum ConsistencyLevel
{
	ANY(HConsistencyLevel.ANY),
	ONE(HConsistencyLevel.ONE),
	TWO(HConsistencyLevel.TWO),
	THREE(HConsistencyLevel.THREE),
	QUORUM(HConsistencyLevel.QUORUM),
	EACH_QUORUM(HConsistencyLevel.EACH_QUORUM),
	LOCAL_QUORUM(HConsistencyLevel.LOCAL_QUORUM),
	ALL(HConsistencyLevel.ALL);

	private final HConsistencyLevel hectorConsistencyLevel;

	private ConsistencyLevel(HConsistencyLevel hectorConsistencyLevel) {
		this.hectorConsistencyLevel = hectorConsistencyLevel;
	}

	public HConsistencyLevel getHectorLevel()
	{
		return this.hectorConsistencyLevel;
	}

	public static ConsistencyLevel convertFromHectorLevel(HConsistencyLevel hectorLevel)
	{
		for (ConsistencyLevel consistencyLevel : ConsistencyLevel.values())
		{
			if (consistencyLevel.getHectorLevel() == hectorLevel)
			{
				return consistencyLevel;
			}
		}
		throw new IllegalArgumentException(
				"No matching Achilles Consistency Level for Hector level '" + hectorLevel.name()
						+ "'");
	}
}
