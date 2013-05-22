package info.archinnov.achilles.context;

import static org.mockito.Mockito.verify;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.type.ConsistencyLevel;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftConsistencyContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftConsistencyContextTest
{
	@InjectMocks
	private ThriftConsistencyContext context;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Test
	public void should_set_read_consistency_level() throws Exception
	{
		context.setReadConsistencyLevel(ConsistencyLevel.ONE);
		verify(policy).setCurrentReadLevel(ConsistencyLevel.ONE);
	}

	@Test
	public void should_set_write_consistency_level() throws Exception
	{
		context.setWriteConsistencyLevel(ConsistencyLevel.ONE);
		verify(policy).setCurrentWriteLevel(ConsistencyLevel.ONE);
	}

	@Test
	public void should_reinit_consistency_levels() throws Exception
	{
		context.reinitConsistencyLevels();
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}
}
