/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

@RunWith(MockitoJUnitRunner.class)
public class ThriftConsistencyContextTest {
	@InjectMocks
	private ThriftConsistencyContext context;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Mock
	private SafeExecutionContext<String> executionContext;

	@Before
	public void setUp() {
		when(executionContext.execute()).thenReturn("result");
	}

	@Test
	public void should_set_read_consistency_level() throws Exception {
		context.setConsistencyLevel(ONE);
		verify(policy).setCurrentReadLevel(ConsistencyLevel.ONE);
	}

	@Test
	public void should_not_set_read_consistency_level_when_null()
			throws Exception {
		context.setConsistencyLevel(null);
		verifyZeroInteractions(policy);
	}

	@Test
	public void should_reinit_consistency_levels() throws Exception {
		context.reinitConsistencyLevels();
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_read_consistency_level() throws Exception {

		context = new ThriftConsistencyContext(policy, ALL);
		String result = context
				.executeWithReadConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentReadLevel(ALL);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_runtime_read_consistency_level()
			throws Exception {

		context = new ThriftConsistencyContext(policy, null);
		String result = context.executeWithReadConsistencyLevel(
				executionContext, QUORUM);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentReadLevel(QUORUM);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_no_runtime_read_consistency_level()
			throws Exception {

		context = new ThriftConsistencyContext(policy, null);
		String result = context.executeWithReadConsistencyLevel(
				executionContext, null);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}

	@Test
	public void should_execute_with_no_read_consistency_level()
			throws Exception {
		context = new ThriftConsistencyContext(policy, null);
		String result = context
				.executeWithReadConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}

	@Test
	public void should_execute_with_write_consistency_level() throws Exception {

		context = new ThriftConsistencyContext(policy, ALL);
		String result = context
				.executeWithWriteConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentWriteLevel(ALL);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_no_write_consistency_level()
			throws Exception {
		context = new ThriftConsistencyContext(policy, null);
		String result = context
				.executeWithWriteConsistencyLevel(executionContext);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}

	@Test
	public void should_execute_with_runtime_write_consistency_level()
			throws Exception {
		context = new ThriftConsistencyContext(policy, null);
		String result = context.executeWithWriteConsistencyLevel(
				executionContext, QUORUM);

		assertThat(result).isEqualTo("result");

		verify(policy).setCurrentWriteLevel(QUORUM);
		verify(policy).reinitCurrentConsistencyLevels();
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_execute_with_no_runtime_write_consistency_level()
			throws Exception {

		context = new ThriftConsistencyContext(policy, null);
		String result = context.executeWithWriteConsistencyLevel(
				executionContext, null);

		assertThat(result).isEqualTo("result");

		verifyZeroInteractions(policy);
	}
}
