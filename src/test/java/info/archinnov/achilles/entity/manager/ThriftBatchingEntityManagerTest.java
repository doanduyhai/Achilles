package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static org.mockito.Mockito.verify;
import info.archinnov.achilles.entity.context.BatchingFlushContext;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * BatchingThriftEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftBatchingEntityManagerTest
{
	@InjectMocks
	private ThriftBatchingEntityManager em;

	@Mock
	private BatchingFlushContext flushContext;

	@Before
	public void setUp()
	{
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

}
