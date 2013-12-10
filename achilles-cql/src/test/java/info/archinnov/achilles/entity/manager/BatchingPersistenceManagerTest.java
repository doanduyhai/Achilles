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
import info.archinnov.achilles.context.BatchingFlushContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
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
public class BatchingPersistenceManagerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private BatchingPersistenceManager manager;

	@Mock
	private PersistenceContextFactory contextFactory;

	@Mock
	private DaoContext daoContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private BatchingFlushContext flushContext;

	@Mock
	private PersistenceManagerFactory pmf;

	@Captor
	private ArgumentCaptor<ConsistencyLevel> consistencyCaptor;

	@Before
	public void setUp() {
		manager = new BatchingPersistenceManager(null, contextFactory, daoContext, configContext);
		Whitebox.setInternalState(manager, BatchingFlushContext.class, flushContext);
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
		verify(flushContext, times(2)).setConsistencyLevel(consistencyCaptor.capture());

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
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.persist(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
	}

	@Test
	public void should_exception_when_merge_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.merge(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
	}

	@Test
	public void should_exception_when_remove_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.remove(new CompleteBean(), ONE);
	}

	@Test
	public void should_exception_when_find_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.find(CompleteBean.class, 11L, ONE);
	}

	@Test
	public void should_exception_when_getReference_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.getReference(CompleteBean.class, 11L, ONE);
	}

	@Test
	public void should_exception_when_refresh_with_consistency() throws Exception {
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

		manager.refresh(new CompleteBean(), ONE);
	}

	@Test
	public void should_init_persistence_context_with_entity() throws Exception {
		// Given
		Object entity = new Object();
		Options options = OptionsBuilder.noOptions();
		PersistenceContext context = mock(PersistenceContext.class);

		// When
		when(contextFactory.newContextWithFlushContext(entity, options, flushContext)).thenReturn(context);

		PersistenceContext actual = manager.initPersistenceContext(entity, options);

		// Then
		assertThat(actual).isSameAs(context);
	}

	@Test
	public void should_init_persistence_context_with_primary_key() throws Exception {
		// Given
		Object primaryKey = new Object();
		Options options = OptionsBuilder.noOptions();
		PersistenceContext context = mock(PersistenceContext.class);

		// When
		when(contextFactory.newContextWithFlushContext(Object.class, primaryKey, options, flushContext)).thenReturn(
				context);

		PersistenceContext actual = manager.initPersistenceContext(Object.class, primaryKey, options);

		// Then
		assertThat(actual).isSameAs(context);
	}
}
