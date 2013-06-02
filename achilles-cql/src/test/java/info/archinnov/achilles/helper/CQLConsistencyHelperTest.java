package info.archinnov.achilles.helper;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.ConsistencyLevel;

import org.junit.Test;

/**
 * CQLConsistencyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLConsistencyHelperTest
{
	@Test
	public void should_get_hector_level_from_achilles_level() throws Exception
	{
		assertThat(CQLConsistencyHelper.getCQLLevel(ConsistencyLevel.EACH_QUORUM)).isEqualTo(
				com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
	}
}
