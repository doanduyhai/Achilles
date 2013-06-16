package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.type.ConsistencyLevel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;

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

	@Mock
	private SafeExecutionContext<String> executionContext;

	@Before
	public void setUp()
	{
		when(executionContext.execute()).thenReturn("result");
	}

	@Test
	public void should_set_read_consistency_level() throws Exception
	{
		context.setReadConsistencyLevel(Optional.fromNullable(ONE));
		verify(policy).setCurrentReadLevel(ConsistencyLevel.ONE);
	}

	@Test
	public void should_not_set_read_consistency_level_when_null() throws Exception
	{
		context.setReadConsistencyLevel(Optional.<ConsistencyLevel> absent());
		verifyZeroInteractions(policy);
	}

	@Test
	public void should_set_write_consistency_level() throws Exception
	{
		context.setWriteConsistencyLevel(Optional.fromNullable(ONE));
		verify(policy).setCurrentWriteLevel(ConsistencyLevel.ONE);
	}

	@Test
	public void should_not_set_write_consistency_level_when_null() throws Exception
	{
		context.setWriteConsistencyLevel(Optional.<ConsistencyLevel> absent());
		verifyZeroInteractions(policy);
	}

	@Test
	public void should_reinit_consistency_levels() throws Exception
	{
		context.reinitConsistencyLevels();
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_read_consistency_level() throws Exception
	{

		context = new ThriftConsistencyContext(policy, Optional.fromNullable(ALL),
				Optional.<ConsistencyLevel> absent());
		String result = context.executeWithReadConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentReadLevel(ALL);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_no_read_consistency_level() throws Exception
	{
		context = new ThriftConsistencyContext(policy, Optional.<ConsistencyLevel> absent(),
				Optional.<ConsistencyLevel> absent());
		String result = context.executeWithReadConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}

	@Test
	public void should_execute_with_write_consistency_level() throws Exception
	{

		context = new ThriftConsistencyContext(policy, Optional.<ConsistencyLevel> absent(),
				Optional.fromNullable(ALL));
		String result = context.executeWithWriteConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentWriteLevel(ALL);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_no_write_consistency_level() throws Exception
	{
		context = new ThriftConsistencyContext(policy, Optional.<ConsistencyLevel> absent(),
				Optional.<ConsistencyLevel> absent());
		String result = context.executeWithWriteConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}
}
