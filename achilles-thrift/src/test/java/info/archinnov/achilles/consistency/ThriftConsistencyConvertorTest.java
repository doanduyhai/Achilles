package info.archinnov.achilles.consistency;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyConvertor;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Test;

/**
 * ThriftConsistencyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftConsistencyConvertorTest
{
	@Test
	public void should_get_hector_level_from_achilles_level() throws Exception
	{
		assertThat(ThriftConsistencyConvertor.getHectorLevel(ConsistencyLevel.EACH_QUORUM)).isEqualTo(
				HConsistencyLevel.EACH_QUORUM);
	}

}
