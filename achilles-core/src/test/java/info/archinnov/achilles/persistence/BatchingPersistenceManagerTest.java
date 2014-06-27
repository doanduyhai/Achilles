/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.persistence;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

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

    @Before
    public void setUp() {
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);
        manager = new BatchingPersistenceManager(null, contextFactory, daoContext, configContext,true);
        Whitebox.setInternalState(manager, BatchingFlushContext.class, flushContext);
    }

    @Test
    public void should_start_batch() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        manager.startBatch();

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);

    }

    @Test
    public void should_start_batch_with_consistency_level() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(EACH_QUORUM)).thenReturn(newFlushContext);

        //When
        manager.startBatch(EACH_QUORUM);

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);
    }

    @Test
    public void should_end_batch() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        manager.endBatch();

        //Then
        verify(flushContext).endBatch();
        assertThat(manager.flushContext).isSameAs(newFlushContext);
    }


    @Test
    public void should_clean_batch() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        manager.cleanBatch();

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);
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

        manager.update(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
    }

    @Test
    public void should_exception_when_remove_with_consistency() throws Exception {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

        manager.remove(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
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

        manager.getProxy(CompleteBean.class, 11L, ONE);
    }

    @Test
    public void should_exception_when_refresh_with_consistency() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");

        manager.refresh(new CompleteBean(), ONE);
    }

    @Test
    public void should_init_persistence_context_with_entity() throws Exception {
        // Given
        Object entity = new Object();
        Options options = OptionsBuilder.noOptions();
        PersistenceContext context = mock(PersistenceContext.class);
        PersistenceContext.PersistenceManagerFacade operations = mock(PersistenceContext.PersistenceManagerFacade.class);

        when(contextFactory.newContextWithFlushContext(entity, options, flushContext)).thenReturn(context);
        when(context.getPersistenceManagerFacade()).thenReturn(operations);

        // When
        PersistenceManagerOperations actual = manager.initPersistenceContext(entity, options);

        // Then
        assertThat(actual).isSameAs(operations);
    }

    @Test
    public void should_init_persistence_context_with_primary_key() throws Exception {
        // Given
        Object primaryKey = new Object();
        Options options = OptionsBuilder.noOptions();
        PersistenceContext context = mock(PersistenceContext.class);
        PersistenceContext.PersistenceManagerFacade operations = mock(PersistenceContext.PersistenceManagerFacade.class);

        when(contextFactory.newContextWithFlushContext(Object.class, primaryKey, options, flushContext)).thenReturn(context);
        when(context.getPersistenceManagerFacade()).thenReturn(operations);

        // When
        PersistenceManagerOperations actual = manager.initPersistenceContext(Object.class, primaryKey, options);

        // Then
        assertThat(actual).isSameAs(operations);
    }
}
