package info.archinnov.achilles.helper;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Test;

/**
 * ThriftConsistencyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyHelperTest
{
	@Test
	public void should_get_hector_level_from_achilles_level() throws Exception
	{
		assertThat(ThriftConsistencyHelper.getHectorLevel(ConsistencyLevel.EACH_QUORUM)).isEqualTo(
				HConsistencyLevel.EACH_QUORUM);
	}

}
