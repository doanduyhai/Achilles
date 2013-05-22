package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.context.ThriftBatchingFlushContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.exception.AchillesException;
import integration.tests.entity.CompleteBean;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
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
	private ThriftDaoContext thriftDaoContext;

	@Mock
	private AchillesConfigurationContext configContext;

	@Mock
	private ThriftConsistencyLevelPolicy consistencyPolicy;

	@Mock
	private ThriftBatchingFlushContext flushContext;

	@Mock
	private AchillesEntityManagerFactory emf;

	@Before
	public void setUp()
	{
		when(configContext.getConsistencyPolicy()).thenReturn(consistencyPolicy);
		em = new ThriftBatchingEntityManager(emf, null, thriftDaoContext, configContext);
		Whitebox.setInternalState(em, "flushContext", flushContext);
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
		em.startBatch(EACH_QUORUM, LOCAL_QUORUM);
		verify(flushContext).startBatch();
		verify(flushContext).setReadConsistencyLevel(EACH_QUORUM);
		verify(flushContext).setWriteConsistencyLevel(LOCAL_QUORUM);
	}

	@Test
	public void should_end_batch() throws Exception
	{
		em.endBatch();
		verify(flushContext).endBatch();
	}

	@Test
	public void should_exception_when_persist_with_consistency() throws Exception
	{
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		em.persist(new CompleteBean(), ONE);
	}

	@Test
	public void should_exception_when_merge_with_consistency() throws Exception
	{
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		em.merge(new CompleteBean(), ONE);
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

	@Test
	public void should_remove_safely() throws Exception
	{
		try
		{
			em.remove(new CompleteBean());
		}
		catch (Exception e)
		{
			verify(flushContext).cleanUp();
		}
	}

	@Test
	public void should_get_reference_safely() throws Exception
	{
		try
		{
			em.getReference(CompleteBean.class, null);
		}
		catch (Exception e)
		{
			verify(flushContext).cleanUp();
		}
	}

	@Test
	public void should_refresh_safely() throws Exception
	{
		try
		{
			em.refresh(new CompleteBean());
		}
		catch (Exception e)
		{
			verify(flushContext).cleanUp();
		}
	}

	@Test
	public void should_initialize_entity_safely() throws Exception
	{
		try
		{
			em.initialize(new CompleteBean());
		}
		catch (Exception e)
		{
			verify(flushContext).cleanUp();
		}
	}

	@Test
	public void should_initialize_entity_collection_safely() throws Exception
	{
		try
		{
			em.initialize(Arrays.asList(new CompleteBean()));
		}
		catch (Exception e)
		{
			verify(flushContext).cleanUp();
		}
	}
}
