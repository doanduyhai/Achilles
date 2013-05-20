package info.archinnov.achilles.consistency;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesConsistencyLevelPolicyTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesConsistencyLevelPolicyTest
{

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	private ConsistencyLevel defaultGlobalReadConsistencyLevel;
	private ConsistencyLevel defaultGlobalWriteConsistencyLevel;

	private Map<String, ConsistencyLevel> readCfConsistencyLevels = new HashMap<String, ConsistencyLevel>();
	private Map<String, ConsistencyLevel> writeCfConsistencyLevels = new HashMap<String, ConsistencyLevel>();

	@Before
	public void setUp()
	{
		doCallRealMethod().when(policy).setReadCfConsistencyLevels(
				(Map<String, ConsistencyLevel>) any(Map.class));
		doCallRealMethod().when(policy).setWriteCfConsistencyLevels(
				(Map<String, ConsistencyLevel>) any(Map.class));

		policy.setReadCfConsistencyLevels(readCfConsistencyLevels);
		policy.setWriteCfConsistencyLevels(writeCfConsistencyLevels);

		readCfConsistencyLevels.clear();
		writeCfConsistencyLevels.clear();

		doCallRealMethod().when(policy).getConsistencyLevelForRead(any(String.class));
		doCallRealMethod().when(policy).getConsistencyLevelForWrite(any(String.class));
	}

	@Test
	public void should_get_consistency_for_read_and_write_from_map() throws Exception
	{
		doCallRealMethod().when(policy).setConsistencyLevelForRead(any(ConsistencyLevel.class),
				any(String.class));
		doCallRealMethod().when(policy).setConsistencyLevelForWrite(any(ConsistencyLevel.class),
				any(String.class));

		policy.setConsistencyLevelForRead(ONE, "cf1");
		policy.setConsistencyLevelForWrite(TWO, "cf1");

		assertThat(policy.getConsistencyLevelForRead("cf1")).isEqualTo(ONE);
		assertThat(policy.getConsistencyLevelForWrite("cf1")).isEqualTo(TWO);
	}

	@Test
	public void should_get_consistency_for_read_and_write_from_default() throws Exception
	{
		doCallRealMethod().when(policy).setDefaultGlobalReadConsistencyLevel(
				any(ConsistencyLevel.class));
		doCallRealMethod().when(policy).setDefaultGlobalWriteConsistencyLevel(
				any(ConsistencyLevel.class));

		policy.setDefaultGlobalReadConsistencyLevel(THREE);
		policy.setDefaultGlobalWriteConsistencyLevel(QUORUM);

		assertThat(policy.getConsistencyLevelForRead("cf2")).isEqualTo(THREE);
		assertThat(policy.getConsistencyLevelForWrite("cf2")).isEqualTo(QUORUM);
	}
}
