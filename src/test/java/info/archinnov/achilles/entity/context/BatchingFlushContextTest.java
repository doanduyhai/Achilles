package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.FlushContext.FlushType;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * BatchingFlushContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class BatchingFlushContextTest
{
	@InjectMocks
	private BatchingFlushContext context;

	@Mock
	private CounterDao counterDao;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private GenericColumnFamilyDao<Long, String> cfDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private ConsistencyContext consistencyContext;

	@Mock
	private DaoContext daoContext;

	private Map<String, Pair<Mutator<?>, AbstractDao<?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?>>>();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(context, "consistencyContext", consistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, "daoContext", daoContext);
		mutatorMap.clear();
	}

	@Test
	public void should_start_batch() throws Exception
	{
		context.startBatch();
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_do_nothing_when_flush_called() throws Exception
	{
		context.flush();
		verifyZeroInteractions(entityDao, consistencyContext);
	}

	@Test
	public void should_end_batch() throws Exception
	{
		Pair<Mutator<?>, AbstractDao<?, ?>> pair = new Pair<Mutator<?>, AbstractDao<?, ?>>(mutator,
				entityDao);
		mutatorMap.put("cf", pair);

		context.endBatch();

		verify(entityDao).executeMutator(mutator);
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_get_type() throws Exception
	{
		assertThat(context.type()).isSameAs(FlushType.BATCH);
	}

}
