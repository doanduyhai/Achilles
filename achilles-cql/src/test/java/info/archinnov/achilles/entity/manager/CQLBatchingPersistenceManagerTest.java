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
package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.CQLConsistencyLevelPolicy;
import info.archinnov.achilles.context.CQLBatchingFlushContext;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class CQLBatchingPersistenceManagerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLBatchingPersistenceManager manager;

	@Mock
	private CQLPersistenceContextFactory contextFactory;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private CQLConsistencyLevelPolicy consistencyPolicy;

	@Mock
	private CQLBatchingFlushContext flushContext;

	@Mock
	private PersistenceManagerFactory pmf;

	@Captor
	private ArgumentCaptor<ConsistencyLevel> consistencyCaptor;

	@Before
	public void setUp() {
		when(configContext.getConsistencyPolicy()).thenReturn(consistencyPolicy);
		manager = new CQLBatchingPersistenceManager(null, contextFactory, daoContext, configContext);
		Whitebox.setInternalState(manager, CQLBatchingFlushContext.class, flushContext);
	}

	@Test
	public void should_start_batch() throws Exception {
		manager.startBatch();
		verify(flushContext).startBatch();
	}

	@Test
	public void should_start_batch_with_consistency_level() throws Exception {
		manager.startBatch(EACH_QUORUM);
		verify(flushContext).startBatch();
		verify(flushContext).setConsistencyLevel(consistencyCaptor.capture());

		assertThat(consistencyCaptor.getValue()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_end_batch() throws Exception {
		manager.endBatch();
		verify(flushContext).endBatch();
		verify(flushContext).cleanUp();
	}

	@Test
	public void should_clean_flush_context_when_exception() throws Exception {
		doThrow(new RuntimeException()).when(flushContext).endBatch();
		try {
			manager.endBatch();
		} catch (RuntimeException ex) {
			verify(flushContext).endBatch();
			verify(flushContext).cleanUp();
		}
	}

	@Test
	public void should_clean_batch() throws Exception {
		manager.cleanBatch();
		verify(flushContext).cleanUp();
	}

	@Test
	public void should_exception_when_persist_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.persist(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
	}

	@Test
	public void should_exception_when_merge_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.merge(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
	}

	@Test
	public void should_exception_when_remove_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.remove(new CompleteBean(), ONE);
	}

	@Test
	public void should_exception_when_find_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.find(CompleteBean.class, 11L, ONE);
	}

	@Test
	public void should_exception_when_getReference_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.getReference(CompleteBean.class, 11L, ONE);
	}

	@Test
	public void should_exception_when_refresh_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		manager.refresh(new CompleteBean(), ONE);
	}
}
