package info.archinnov.achilles.entity.type;

import static org.fest.assertions.api.Assertions.assertThat;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Test;

/**
 * ConsistencyLevelTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConsistencyLevelTest
{

	@Test
	public void should_get_hector_level_from_achilles_level() throws Exception
	{
		assertThat(ConsistencyLevel.EACH_QUORUM.getHectorLevel()).isEqualTo(
				HConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_convert_from_hector_level() throws Exception
	{
		assertThat(ConsistencyLevel.convertFromHectorLevel(HConsistencyLevel.THREE)).isEqualTo(
				ConsistencyLevel.THREE);
	}
}
