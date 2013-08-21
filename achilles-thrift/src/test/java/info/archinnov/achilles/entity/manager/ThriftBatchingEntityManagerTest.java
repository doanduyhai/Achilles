package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftBatchingFlushContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
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

/**
 * ThriftBatchingEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftBatchingEntityManagerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ThriftBatchingEntityManager em;

    @Mock
    private ThriftPersistenceContextFactory contextFactory;

    @Mock
    private ThriftDaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private ThriftConsistencyLevelPolicy consistencyPolicy;

    @Mock
    private ThriftBatchingFlushContext flushContext;

    @Mock
    private EntityManagerFactory emf;

    @Captor
    private ArgumentCaptor<ConsistencyLevel> consistencyCaptor;

    @Before
    public void setUp()
    {
        when(configContext.getConsistencyPolicy()).thenReturn(consistencyPolicy);
        em = new ThriftBatchingEntityManager(null, contextFactory, daoContext, configContext);
        Whitebox.setInternalState(em, ThriftBatchingFlushContext.class, flushContext);
    }

    @Test
    public void should_start_batch() throws Exception
    {
        em.startBatch();
        verify(flushContext).startBatch();
    }

    @Test
    public void should_start_batch_with_consistency_level() throws Exception
    {
        em.startBatch(EACH_QUORUM);
        verify(flushContext).startBatch();
        verify(flushContext).setConsistencyLevel(consistencyCaptor.capture());

        assertThat(consistencyCaptor.getValue()).isSameAs(EACH_QUORUM);
    }

    @Test
    public void should_end_batch() throws Exception
    {
        em.endBatch();
        verify(flushContext).endBatch();
    }

    @Test
    public void should_clean_batch() throws Exception
    {
        em.cleanBatch();
        verify(flushContext).cleanUp();
    }

    @Test
    public void should_exception_when_persist_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.persist(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
    }

    @Test
    public void should_exception_when_merge_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.merge(new CompleteBean(), OptionsBuilder.withConsistency(ONE));
    }

    @Test
    public void should_exception_when_remove_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.remove(new CompleteBean(), ONE);
    }

    @Test
    public void should_exception_when_find_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.find(CompleteBean.class, 11L, ONE);
    }

    @Test
    public void should_exception_when_getReference_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.getReference(CompleteBean.class, 11L, ONE);
    }

    @Test
    public void should_exception_when_refresh_with_consistency() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

        em.refresh(new CompleteBean(), ONE);
    }
}
